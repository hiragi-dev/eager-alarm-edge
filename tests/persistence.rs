use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Local};
use eager_alarm_edge::{Alarm, AlarmManager, LogRinger, Ringer};
use uuid::Uuid;

struct CountingRinger;

#[async_trait]
impl Ringer for CountingRinger {
    async fn ring(&self, _alarm: &Alarm) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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
    let saved: Vec<Alarm> = serde_json::from_str(&saved).unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].id, id);
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

    let saved: Vec<Alarm> =
        serde_json::from_str(&fs::read_to_string(&path.0).unwrap()).unwrap();
    assert_eq!(saved.len(), 1);
    assert!(!saved[0].is_enabled);

    handle.delete_alarm(id);
    handle.list_alarms().await;

    let saved: Vec<Alarm> =
        serde_json::from_str(&fs::read_to_string(&path.0).unwrap()).unwrap();
    assert!(saved.is_empty());
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
