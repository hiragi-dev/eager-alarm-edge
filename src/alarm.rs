use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Local, NaiveTime, Weekday};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Alarm {
    pub id: Uuid,
    #[serde(with = "naive_time_hm")]
    pub time: NaiveTime,
    #[serde(default)]
    pub days_of_week: Vec<Weekday>,
    #[serde(default = "default_true")]
    pub is_enabled: bool,
    /// アプリ側（ブラウザ）が管理する停止方法のID。edge側はこの値の意味を
    /// 解釈せず、不透明な文字列としてそのまま保存・返却する。
    #[serde(default)]
    pub stop_method_id: Option<String>,
}

fn default_true() -> bool {
    true
}

pub mod naive_time_hm {
    use super::*;
    use serde::{Deserializer, Serializer, de};

    pub fn serialize<S>(time: &NaiveTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&time.format("%H:%M").to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveTime::parse_from_str(&s, "%H:%M").map_err(de::Error::custom)
    }
}

impl Alarm {
    pub fn next_wakeup_from(&self, now: DateTime<Local>) -> Option<DateTime<Local>> {
        if !self.is_enabled || self.days_of_week.is_empty() {
            return None;
        }

        // 1秒の猶予を持たせることで、`Local::now()` でアラームを作成した直後に
        // `next_wakeup_from` を呼び出しても、わずかに過去になった時刻を正しく
        // "今すぐ発火" として扱えるようにする。
        // 発火後の再スケジュールは必ず +2 秒以降を起点にするため、
        // 同じ日の時刻が再び返ることはない。
        let tolerance = chrono::Duration::seconds(1);

        let mut day = now.date_naive();
        for _ in 0..8 {
            if self.days_of_week.contains(&day.weekday()) {
                if let Some(candidate) = day.and_time(self.time).and_local_timezone(Local).single()
                {
                    // candidate が now より未来、または1秒以内の過去なら発火対象とする
                    if candidate + tolerance > now {
                        // 過去にスケジュールされていた場合は now を発火時刻とする
                        return Some(candidate.max(now));
                    }
                }
            }
            day = day.succ_opt().unwrap_or(day);
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScheduledAlarm {
    alarm_id: Uuid,
    next_wakeup: DateTime<Local>,
}

impl Ord for ScheduledAlarm {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.next_wakeup.cmp(&other.next_wakeup)
    }
}

impl PartialOrd for ScheduledAlarm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// `pause` コマンドによるミュート状態を `Ringer` 側から確認するための共有ハンドル。
/// ミュート期間の記録・更新は `AlarmManager` が行うが、実際にその期間中
/// 出力（ブザー/スピーカー等）を鳴らさないようにする判断は `Ringer` 実装側の責務。
#[derive(Clone, Default)]
pub struct MuteStatus(Arc<Mutex<Option<Instant>>>);

impl MuteStatus {
    /// 現在ミュート期間中かどうかを返す。
    pub fn is_muted(&self) -> bool {
        matches!(*self.0.lock().unwrap(), Some(until) if Instant::now() < until)
    }

    fn set_muted_until(&self, until: Instant) {
        *self.0.lock().unwrap() = Some(until);
    }
}

/// Produces the alarm's real-world output (buzzer, speaker, LED, ...).
#[async_trait]
pub trait Ringer: Send + Sync + 'static {
    async fn ring(&self, alarm: &Alarm, mute: &MuteStatus);
}

pub struct LogRinger;

#[async_trait]
impl Ringer for LogRinger {
    async fn ring(&self, alarm: &Alarm, mute: &MuteStatus) {
        loop {
            if !mute.is_muted() {
                tracing::info!(id = %alarm.id, time = %alarm.time, "alarm ringing");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub enum AlarmCommand {
    Add(Alarm),
    Edit(Alarm),
    Delete(Uuid),
    List(oneshot::Sender<Vec<Alarm>>),
    /// 現在鳴動中のアラーム情報を問い合わせる
    RingingStatus(oneshot::Sender<RingingStatusReply>),
    Pause(Duration),
    StopAll,
}

/// `ringing_status` コマンドへの応答ペイロード
#[derive(Debug, Clone, serde::Serialize)]
pub struct RingingStatusReply {
    /// 現在鳴動中のアラームが 1 件以上ある場合 true
    pub is_ringing: bool,
    /// 鳴動中のアラームの ID 一覧（鳴動中でなければ空配列）
    pub ringing_ids: Vec<Uuid>,
}

pub struct AlarmManager {
    alarms: HashMap<Uuid, Alarm>,
    queue: BinaryHeap<Reverse<ScheduledAlarm>>,
    ringing: HashMap<Uuid, JoinHandle<()>>,
    mute_status: MuteStatus,
    ringer: Arc<dyn Ringer>,
    store_path: Option<PathBuf>,
    cmd_tx: mpsc::UnboundedSender<AlarmCommand>,
    cmd_rx: mpsc::UnboundedReceiver<AlarmCommand>,
}

/// ファイルへ読み書きする永続化状態。アラーム設定に加えて、プロセス終了時点で
/// 鳴動中だったアラームの ID も保存し、次回起動時に鳴動を再開できるようにする。
#[derive(Debug, Default, Deserialize)]
struct PersistedState {
    alarms: Vec<Alarm>,
    #[serde(default)]
    ringing_ids: Vec<Uuid>,
}

/// [`PersistedState`] の書き込み専用版。アラームの clone を避けるため参照で持つ。
#[derive(Serialize)]
struct PersistedStateRef<'a> {
    alarms: Vec<&'a Alarm>,
    ringing_ids: &'a [Uuid],
}

/// 保存されている状態を読み込む。ファイルが存在しない場合は空で開始し、
/// 壊れている場合はログに残した上で空で開始する（永続化の失敗でアラーム自体が
/// 起動できなくなるのを避けるため）。旧形式（アラームの配列のみ）のファイルも
/// 引き続き読み込める（鳴動中アラームは無しとして扱う）。
fn load_state(path: &Path) -> PersistedState {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return PersistedState::default(),
        Err(e) => {
            tracing::error!(error = %e, path = %path.display(), "failed to read alarms file; starting with no alarms");
            return PersistedState::default();
        }
    };

    if let Ok(state) = serde_json::from_slice::<PersistedState>(&bytes) {
        return state;
    }

    // 旧形式（`Vec<Alarm>` そのもの）へのフォールバック。
    match serde_json::from_slice::<Vec<Alarm>>(&bytes) {
        Ok(alarms) => PersistedState {
            alarms,
            ringing_ids: Vec::new(),
        },
        Err(e) => {
            tracing::error!(error = %e, path = %path.display(), "failed to parse alarms file; starting with no alarms");
            PersistedState::default()
        }
    }
}

/// 状態をファイルへ書き込む。同じディレクトリの一時ファイルに書いてから
/// rename することで、書き込み途中の電源断等でファイルが壊れないようにする。
fn save_state(path: &Path, state: &PersistedStateRef) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let json = serde_json::to_vec_pretty(state)?;

    let mut tmp_path = path.as_os_str().to_os_string();
    tmp_path.push(".tmp");
    let tmp_path = PathBuf::from(tmp_path);

    std::fs::write(&tmp_path, json)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

impl Drop for AlarmManager {
    fn drop(&mut self) {
        for handle in self.ringing.values() {
            handle.abort();
        }
    }
}

#[derive(Clone)]
pub struct AlarmHandle {
    cmd_tx: mpsc::UnboundedSender<AlarmCommand>,
}

impl AlarmHandle {
    pub fn add_alarm(&self, alarm: Alarm) -> Uuid {
        let id = alarm.id;
        let _ = self.cmd_tx.send(AlarmCommand::Add(alarm));
        id
    }

    pub fn edit_alarm(&self, alarm: Alarm) {
        let _ = self.cmd_tx.send(AlarmCommand::Edit(alarm));
    }

    pub fn delete_alarm(&self, id: Uuid) {
        let _ = self.cmd_tx.send(AlarmCommand::Delete(id));
    }

    pub async fn list_alarms(&self) -> Vec<Alarm> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.cmd_tx.send(AlarmCommand::List(reply_tx)).is_err() {
            return Vec::new();
        }
        reply_rx.await.unwrap_or_default()
    }

    /// 現在鳴動中のアラームの状態を返す。
    /// `AlarmManager::run` が屈1回とりで問い合わせに応答するため、async となる。
    pub async fn get_ringing_status(&self) -> RingingStatusReply {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .cmd_tx
            .send(AlarmCommand::RingingStatus(reply_tx))
            .is_err()
        {
            return RingingStatusReply {
                is_ringing: false,
                ringing_ids: Vec::new(),
            };
        }
        reply_rx.await.unwrap_or(RingingStatusReply {
            is_ringing: false,
            ringing_ids: Vec::new(),
        })
    }

    pub fn pause(&self, duration: Duration) {
        let _ = self.cmd_tx.send(AlarmCommand::Pause(duration));
    }

    pub fn stop_all(&self) {
        let _ = self.cmd_tx.send(AlarmCommand::StopAll);
    }
}

impl Default for AlarmManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AlarmManager {
    pub fn new() -> Self {
        Self::with_ringer(Arc::new(LogRinger))
    }

    pub fn with_ringer(ringer: Arc<dyn Ringer>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        AlarmManager {
            alarms: HashMap::new(),
            queue: BinaryHeap::new(),
            ringing: HashMap::new(),
            mute_status: MuteStatus::default(),
            ringer,
            store_path: None,
            cmd_tx,
            cmd_rx,
        }
    }

    /// `path` に保存されているアラーム設定を読み込んで復元し、以後の
    /// add/edit/delete のたびに同じファイルへ書き戻す `AlarmManager` を作る。
    pub fn with_store(path: impl Into<PathBuf>) -> Self {
        Self::with_ringer_and_store(Arc::new(LogRinger), path)
    }

    /// [`with_store`](Self::with_store) の `Ringer` を差し替え可能な版。
    pub fn with_ringer_and_store(ringer: Arc<dyn Ringer>, path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let state = load_state(&path);
        let alarms: HashMap<Uuid, Alarm> = state.alarms.into_iter().map(|a| (a.id, a)).collect();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let mut manager = AlarmManager {
            alarms,
            queue: BinaryHeap::new(),
            ringing: HashMap::new(),
            mute_status: MuteStatus::default(),
            ringer,
            store_path: Some(path),
            cmd_tx,
            cmd_rx,
        };
        manager.rebuild_queue();

        // プロセス終了時点で鳴動中だったアラームは、次回起動時に即座に鳴動を再開する。
        for id in state.ringing_ids {
            if manager.alarms.contains_key(&id) {
                manager.start_ringing(id);
            }
        }

        manager
    }

    /// `store_path` が設定されていれば現在のアラーム設定と鳴動状態をファイルへ書き込む。
    fn persist(&self) {
        let Some(path) = &self.store_path else {
            return;
        };
        let alarms: Vec<&Alarm> = self.alarms.values().collect();
        let ringing_ids: Vec<Uuid> = self.ringing.keys().copied().collect();
        let state = PersistedStateRef {
            alarms,
            ringing_ids: &ringing_ids,
        };
        if let Err(e) = save_state(path, &state) {
            tracing::error!(error = %e, path = %path.display(), "failed to persist alarms");
        }
    }

    pub fn handle(&self) -> AlarmHandle {
        AlarmHandle {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    fn schedule_alarm(&mut self, alarm_id: Uuid, now: DateTime<Local>) {
        if let Some(alarm) = self.alarms.get(&alarm_id) {
            if let Some(next_wakeup) = alarm.next_wakeup_from(now) {
                self.queue.push(Reverse(ScheduledAlarm {
                    alarm_id,
                    next_wakeup,
                }));
            }
        }
    }

    fn deadline_for(wakeup_time: DateTime<Local>) -> Instant {
        let dur = (wakeup_time - Local::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        Instant::now() + dur
    }

    fn start_ringing(&mut self, alarm_id: Uuid) {
        if let Some(alarm) = self.alarms.get(&alarm_id) {
            let ringer = Arc::clone(&self.ringer);
            let ringing_alarm = alarm.clone();
            let mute_status = self.mute_status.clone();
            let join = tokio::spawn(async move { ringer.ring(&ringing_alarm, &mute_status).await });
            self.ringing.insert(alarm_id, join);
        }
    }

    fn stop_ringing(&mut self, id: Uuid) {
        if let Some(handle) = self.ringing.remove(&id) {
            handle.abort();
        }
    }

    fn rebuild_queue(&mut self) {
        self.queue.clear();
        let now = Local::now();
        let ids: Vec<Uuid> = self.alarms.keys().copied().collect();
        for id in ids {
            self.schedule_alarm(id, now);
        }
    }

    fn handle_command(&mut self, cmd: AlarmCommand) {
        match cmd {
            AlarmCommand::Add(alarm) => {
                let id = alarm.id;
                self.alarms.insert(id, alarm);
                self.schedule_alarm(id, Local::now());
                self.persist();
            }
            AlarmCommand::Edit(alarm) => {
                let id = alarm.id;
                self.alarms.insert(id, alarm);
                self.stop_ringing(id); // 鳴っている最中なら止める
                self.rebuild_queue(); // 編集されたのでキューを再構築するのが手っ取り早い
                self.persist();
            }
            AlarmCommand::Delete(id) => {
                self.alarms.remove(&id);
                self.stop_ringing(id);
                self.rebuild_queue();
                self.persist();
            }
            AlarmCommand::List(reply_tx) => {
                let mut alarms_vec: Vec<Alarm> = self.alarms.values().cloned().collect();
                alarms_vec.sort_by_key(|a| a.time);
                let _ = reply_tx.send(alarms_vec);
            }
            AlarmCommand::RingingStatus(reply_tx) => {
                let ringing_ids: Vec<Uuid> = self.ringing.keys().copied().collect();
                let _ = reply_tx.send(RingingStatusReply {
                    is_ringing: !ringing_ids.is_empty(),
                    ringing_ids,
                });
            }
            AlarmCommand::Pause(duration) => {
                // ミュート期間を記録するだけ。実際に出力を止めるかどうかは、
                // 鳴動中/これから鳴動する各 Ringer が MuteStatus を見て判断する。
                self.mute_status.set_muted_until(Instant::now() + duration);
            }
            AlarmCommand::StopAll => {
                // 先に鳴動中のIDを記録してから停止する（stop_ringing が ringing から除去するため）
                let ringing_ids: Vec<Uuid> = self.ringing.keys().copied().collect();
                for &id in &ringing_ids {
                    self.stop_ringing(id);
                }

                // キューを再構築する。
                // 直前に止めたアラームは +2 秒後を起点にして next_wakeup_from の
                // 1 秒猶予内に即再起動しないようにする。
                // それ以外の待機中アラームは Local::now() を起点に通常スケジュール。
                let now = Local::now();
                let after_stop = now + chrono::Duration::seconds(2);
                let stopped: std::collections::HashSet<Uuid> = ringing_ids.into_iter().collect();

                self.queue.clear();
                let ids: Vec<Uuid> = self.alarms.keys().copied().collect();
                for id in ids {
                    let base = if stopped.contains(&id) {
                        after_stop
                    } else {
                        now
                    };
                    self.schedule_alarm(id, base);
                }

                self.persist();
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            // キューの先頭から、まだ有効なエントリ（アラームが存在するもの）を探す。
            // edit/delete では rebuild_queue() を呼ぶため、基本的に先頭は常に有効だが、
            // アラームが削除された後に残った古いエントリを読み飛ばすための保険として残す。
            while self
                .queue
                .peek()
                .is_some_and(|Reverse(s)| !self.alarms.contains_key(&s.alarm_id))
            {
                self.queue.pop();
            }

            let Some(Reverse(next_scheduled)) = self.queue.peek() else {
                // スケジュール済みのアラームがない場合は次のコマンドを待つ
                match self.cmd_rx.recv().await {
                    Some(cmd) => self.handle_command(cmd),
                    None => return,
                }
                continue;
            };

            let deadline = Self::deadline_for(next_scheduled.next_wakeup);

            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    if let Some(Reverse(scheduled)) = self.queue.pop() {
                        self.start_ringing(scheduled.alarm_id);

                        // 発火後は +2 秒を起点に次の発火日時を計算する。
                        // +2 秒により next_wakeup_from の 1 秒猶予を超えるため、
                        // 同じ日の時刻が再度選ばれることはなく、次の曜日へ進む。
                        let next_base = scheduled.next_wakeup + chrono::Duration::seconds(2);
                        self.schedule_alarm(scheduled.alarm_id, next_base);

                        // 鳴動開始をファイルへ反映する。ここで永続化しておかないと、
                        // 鳴動中にプロセスが落ちた場合に次回起動時に鳴動を再開できない。
                        self.persist();
                    }
                }
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd),
                        None => return,
                    }
                }
            }
        }
    }
}
