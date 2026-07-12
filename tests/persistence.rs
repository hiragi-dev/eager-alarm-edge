use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Local};
use eager_alarm_edge::{Alarm, AlarmManager, LogRinger, MuteStatus, Ringer};
use uuid::Uuid;

struct CountingRinger;

#[async_trait]
impl Ringer for CountingRinger {
    async fn ring(&self, _alarm: &Alarm, _mute: &MuteStatus) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

struct TickingRinger {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl Ringer for TickingRinger {
    async fn ring(&self, _alarm: &Alarm, _mute: &MuteStatus) {
        loop {
            self.count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// アラームファイルの保存フォーマット（テストから直接ファイル内容を検証するためのもの）。
#[derive(serde::Deserialize)]
struct SavedFile {
    alarms: Vec<Alarm>,
    #[serde(default)]
    ringing_ids: Vec<Uuid>,
}

fn create_alarm(time: DateTime<Local>) -> Alarm {
    Alarm {
        id: Uuid::new_v4(),
        time: time.time(),
        days_of_week: vec![time.weekday()],
        is_enabled: true,
        stop_method_id: None,
    }
}

/// テスト用の一意な一時ファイルパス。Drop でファイルを削除する。
struct TempStorePath(PathBuf);

impl TempStorePath {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("eager-alarm-test-{}.json", Uuid::new_v4())))
    }
}

impl Drop for TempStorePath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn add_persists_to_file() {
    let path = TempStorePath::new();
    let mut manager = AlarmManager::with_ringer_and_store(Arc::new(LogRinger), path.0.clone());
    let handle = manager.handle();
    let _task = tokio::spawn(async move { manager.run().await });

    let alarm = create_alarm(Local::now() + chrono::Duration::minutes(30));
    let id = handle.add_alarm(alarm.clone());
    // List は oneshot 応答なので、これを待てば直前の Add の処理完了も保証される。
    handle.list_alarms().await;

    let saved = fs::read_to_string(&path.0).expect("alarms file should exist after add");
    let saved: SavedFile = serde_json::from_str(&saved).unwrap();
    assert_eq!(saved.alarms.len(), 1);
    assert_eq!(saved.alarms[0].id, id);
}

#[tokio::test]
async fn edit_and_delete_update_file() {
    let path = TempStorePath::new();
    let mut manager = AlarmManager::with_ringer_and_store(Arc::new(LogRinger), path.0.clone());
    let handle = manager.handle();
    let _task = tokio::spawn(async move { manager.run().await });

    let mut alarm = create_alarm(Local::now() + chrono::Duration::minutes(30));
    let id = handle.add_alarm(alarm.clone());
    handle.list_alarms().await;

    alarm.id = id;
    alarm.is_enabled = false;
    handle.edit_alarm(alarm);
    let listed = handle.list_alarms().await;
    assert_eq!(listed.len(), 1);
    assert!(!listed[0].is_enabled);

    let saved: SavedFile = serde_json::from_str(&fs::read_to_string(&path.0).unwrap()).unwrap();
    assert_eq!(saved.alarms.len(), 1);
    assert!(!saved.alarms[0].is_enabled);

    handle.delete_alarm(id);
    handle.list_alarms().await;

    let saved: SavedFile = serde_json::from_str(&fs::read_to_string(&path.0).unwrap()).unwrap();
    assert!(saved.alarms.is_empty());
}

#[tokio::test]
async fn restarted_manager_restores_alarms_from_file() {
    let path = TempStorePath::new();

    {
        let mut manager =
            AlarmManager::with_ringer_and_store(Arc::new(CountingRinger), path.0.clone());
        let handle = manager.handle();
        let task = tokio::spawn(async move { manager.run().await });

        let alarm = create_alarm(Local::now() + chrono::Duration::minutes(30));
        handle.add_alarm(alarm);
        handle.list_alarms().await;
        task.abort();
    }

    let mut manager = AlarmManager::with_ringer_and_store(Arc::new(CountingRinger), path.0.clone());
    let handle = manager.handle();
    let _task = tokio::spawn(async move { manager.run().await });

    let restored = handle.list_alarms().await;
    assert_eq!(restored.len(), 1);
}

#[tokio::test]
async fn missing_file_starts_empty() {
    let path = TempStorePath::new();
    // わざとファイルを作らない。

    let mut manager = AlarmManager::with_ringer_and_store(Arc::new(LogRinger), path.0.clone());
    let handle = manager.handle();
    let _task = tokio::spawn(async move { manager.run().await });

    assert!(handle.list_alarms().await.is_empty());
}

#[tokio::test]
async fn corrupt_file_starts_empty_instead_of_panicking() {
    let path = TempStorePath::new();
    fs::write(&path.0, b"not valid json").unwrap();

    let mut manager = AlarmManager::with_ringer_and_store(Arc::new(LogRinger), path.0.clone());
    let handle = manager.handle();
    let _task = tokio::spawn(async move { manager.run().await });

    assert!(handle.list_alarms().await.is_empty());
}

#[tokio::test]
async fn stop_method_id_round_trips_through_persistence() {
    let path = TempStorePath::new();
    let mut manager = AlarmManager::with_ringer_and_store(Arc::new(LogRinger), path.0.clone());
    let handle = manager.handle();
    let _task = tokio::spawn(async move { manager.run().await });

    let mut alarm = create_alarm(Local::now() + chrono::Duration::minutes(30));
    alarm.stop_method_id = Some("geo:office".to_string());
    let id = alarm.id;
    handle.add_alarm(alarm);
    handle.list_alarms().await;

    let saved: SavedFile = serde_json::from_str(&fs::read_to_string(&path.0).unwrap()).unwrap();
    assert_eq!(
        saved
            .alarms
            .iter()
            .find(|a| a.id == id)
            .unwrap()
            .stop_method_id
            .as_deref(),
        Some("geo:office")
    );
}

#[tokio::test]
async fn legacy_file_without_stop_method_id_loads_as_null() {
    let path = TempStorePath::new();
    let legacy_alarm_id = Uuid::new_v4();
    let legacy_json = format!(
        r#"[{{"id":"{legacy_alarm_id}","time":"07:30","days_of_week":["Mon"],"is_enabled":true}}]"#
    );
    fs::write(&path.0, legacy_json).unwrap();

    let mut manager = AlarmManager::with_ringer_and_store(Arc::new(LogRinger), path.0.clone());
    let handle = manager.handle();
    let _task = tokio::spawn(async move { manager.run().await });

    let listed = handle.list_alarms().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].stop_method_id, None);
}

#[tokio::test]
async fn ringing_alarm_is_persisted_and_resumes_after_restart() {
    let path = TempStorePath::new();
    let id;

    {
        let count = Arc::new(AtomicUsize::new(0));
        let mut manager = AlarmManager::with_ringer_and_store(
            Arc::new(TickingRinger {
                count: count.clone(),
            }),
            path.0.clone(),
        );
        let handle = manager.handle();
        let task = tokio::spawn(async move { manager.run().await });

        let alarm = create_alarm(Local::now());
        id = handle.add_alarm(alarm);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            count.load(Ordering::SeqCst) >= 1,
            "alarm should be ringing before shutdown"
        );

        let saved: SavedFile = serde_json::from_str(&fs::read_to_string(&path.0).unwrap()).unwrap();
        assert_eq!(saved.ringing_ids, vec![id]);

        // プロセスが落ちたことを模す。AlarmManager の Drop が鳴動タスクも止める。
        task.abort();
    }

    let count_after_restart = Arc::new(AtomicUsize::new(0));
    let mut manager = AlarmManager::with_ringer_and_store(
        Arc::new(TickingRinger {
            count: count_after_restart.clone(),
        }),
        path.0.clone(),
    );
    let _task = tokio::spawn(async move { manager.run().await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        count_after_restart.load(Ordering::SeqCst) >= 1,
        "alarm should resume ringing immediately after restart"
    );
}
