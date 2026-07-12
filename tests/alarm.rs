use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Local};
use eager_alarm_edge::{Alarm, AlarmManager, LogRinger, Ringer};
use uuid::Uuid;

struct CountingRinger {
    count: Arc<AtomicUsize>,
    interval: Duration,
}

#[async_trait]
impl Ringer for CountingRinger {
    async fn ring(&self, _alarm: &Alarm) {
        loop {
            self.count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.interval).await;
        }
    }
}

struct RecordingRinger {
    fired: Arc<Mutex<Vec<Uuid>>>,
}

#[async_trait]
impl Ringer for RecordingRinger {
    async fn ring(&self, alarm: &Alarm) {
        self.fired.lock().unwrap().push(alarm.id);
    }
}

fn create_alarm(time: DateTime<Local>) -> Alarm {
    Alarm {
        id: Uuid::new_v4(),
        time: time.time(),
        days_of_week: vec![time.weekday()],
        is_enabled: true,
    }
}

#[tokio::test]
async fn add_and_list_a_pending_alarm() {
    let mut manager = AlarmManager::with_ringer(Arc::new(LogRinger));
    let handle = manager.handle();
    let task = tokio::spawn(async move { manager.run().await });

    let alarm = create_alarm(Local::now() + chrono::Duration::seconds(30));
    let id = handle.add_alarm(alarm);
    let listed = handle.list_alarms().await;

    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);

    task.abort();
}

#[tokio::test]
async fn delete_removes_a_pending_alarm_before_it_fires() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut manager = AlarmManager::with_ringer(Arc::new(CountingRinger {
        count: count.clone(),
        interval: Duration::from_millis(50),
    }));
    let handle = manager.handle();
    let task = tokio::spawn(async move { manager.run().await });

    let alarm = create_alarm(Local::now() + chrono::Duration::milliseconds(200));
    let id = handle.add_alarm(alarm);
    handle.delete_alarm(id);

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(handle.list_alarms().await.is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 0);

    task.abort();
}

#[tokio::test]
async fn alarms_fire_in_wakeup_time_order_regardless_of_insertion_order() {
    let fired = Arc::new(Mutex::new(Vec::new()));
    let mut manager = AlarmManager::with_ringer(Arc::new(RecordingRinger {
        fired: fired.clone(),
    }));
    let handle = manager.handle();
    let task = tokio::spawn(async move { manager.run().await });

    let alarm_300 = create_alarm(Local::now() + chrono::Duration::milliseconds(300));
    let id_300 = handle.add_alarm(alarm_300);

    let alarm_100 = create_alarm(Local::now() + chrono::Duration::milliseconds(100));
    let id_100 = handle.add_alarm(alarm_100);

    let alarm_200 = create_alarm(Local::now() + chrono::Duration::milliseconds(200));
    let id_200 = handle.add_alarm(alarm_200);

    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(*fired.lock().unwrap(), vec![id_100, id_200, id_300]);

    for id in [id_100, id_200, id_300] {
        handle.delete_alarm(id);
    }
    task.abort();
}

#[tokio::test]
async fn ringing_alarm_keeps_ringing_until_deleted() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut manager = AlarmManager::with_ringer(Arc::new(CountingRinger {
        count: count.clone(),
        interval: Duration::from_millis(50),
    }));
    let handle = manager.handle();
    let task = tokio::spawn(async move { manager.run().await });

    let alarm = create_alarm(Local::now());
    let id = handle.add_alarm(alarm);

    tokio::time::sleep(Duration::from_millis(220)).await;
    let ticks_before_delete = count.load(Ordering::SeqCst);
    assert!(
        ticks_before_delete >= 3,
        "expected several ticks while ringing, got {ticks_before_delete}"
    );
    assert_eq!(handle.list_alarms().await.len(), 1);

    handle.delete_alarm(id);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(handle.list_alarms().await.is_empty());

    let ticks_at_delete = count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        count.load(Ordering::SeqCst),
        ticks_at_delete,
        "ring task should stop ticking once deleted"
    );

    task.abort();
}

#[tokio::test]
async fn stop_all_silences_ringing_alarms_but_leaves_pending_ones_scheduled() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut manager = AlarmManager::with_ringer(Arc::new(CountingRinger {
        count: count.clone(),
        interval: Duration::from_millis(50),
    }));
    let handle = manager.handle();
    let task = tokio::spawn(async move { manager.run().await });

    let ringing_alarm = create_alarm(Local::now());
    let ringing_id = handle.add_alarm(ringing_alarm);

    let pending_alarm = create_alarm(Local::now() + chrono::Duration::seconds(30));
    let pending_id = handle.add_alarm(pending_alarm);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        count.load(Ordering::SeqCst) >= 1,
        "should be ringing by now"
    );

    handle.stop_all();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let listed = handle.list_alarms().await;
    assert_eq!(
        listed.len(),
        2,
        "stop_all leaves the stopped alarm as well as pending ones in the list"
    );

    let ticks_at_stop = count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        count.load(Ordering::SeqCst),
        ticks_at_stop,
        "stop_all should abort the ringing task, not just unlist it"
    );

    handle.delete_alarm(ringing_id);
    handle.delete_alarm(pending_id);
    task.abort();
}

#[tokio::test]
async fn pause_overwrites_rather_than_accumulates_and_then_auto_resumes() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut manager = AlarmManager::with_ringer(Arc::new(CountingRinger {
        count: count.clone(),
        interval: Duration::from_millis(50),
    }));
    let handle = manager.handle();
    let task = tokio::spawn(async move { manager.run().await });

    let alarm = create_alarm(Local::now() + chrono::Duration::milliseconds(200));
    let id = handle.add_alarm(alarm);

    for _ in 0..6 {
        handle.pause(Duration::from_millis(300));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(count.load(Ordering::SeqCst), 0);

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        count.load(Ordering::SeqCst) >= 1,
        "alarm should have started ringing again once the mute expired"
    );

    handle.delete_alarm(id);
    task.abort();
}
