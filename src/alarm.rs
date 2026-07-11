use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Local};
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
    pub wakeup_time: DateTime<Local>,
}

impl Ord for Alarm {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.wakeup_time.cmp(&other.wakeup_time)
    }
}

impl PartialOrd for Alarm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Produces the alarm's real-world output (buzzer, speaker, LED, ...).
///
/// Called once when the alarm starts ringing; the implementation owns its
/// pacing entirely — loop forever, ring a fixed number of times and
/// return, escalate over time, whatever fits the output. `AlarmManager`
/// only starts this future (on wakeup) and aborts it (on delete); it has
/// no opinion on how the ringer behaves in between.
#[async_trait]
pub trait Ringer: Send + Sync + 'static {
    async fn ring(&self, alarm: &Alarm);
}

/// Placeholder [`Ringer`] used until real hardware output is wired up:
/// logs, once a second, that the alarm is still going off.
pub struct LogRinger;

#[async_trait]
impl Ringer for LogRinger {
    async fn ring(&self, alarm: &Alarm) {
        loop {
            tracing::info!(id = %alarm.id, wakeup_time = %alarm.wakeup_time, "alarm ringing");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

enum AlarmCommand {
    Add(Alarm),
    Delete(Uuid),
    List(oneshot::Sender<Vec<Alarm>>),
    Pause(Duration),
    StopAll,
}

pub struct AlarmManager {
    queue: BinaryHeap<Reverse<Alarm>>,
    // Alarms whose wakeup_time has passed and that are actively ringing,
    // keyed by id. Each has a background task running its Ringer until
    // deleted.
    ringing: HashMap<Uuid, (Alarm, JoinHandle<()>)>,
    // Overwritten (not accumulated) by each Pause command, so repeated
    // pauses from an ongoing trigger (e.g. footsteps) just push this out
    // rather than stacking indefinitely.
    muted_until: Option<Instant>,
    ringer: Arc<dyn Ringer>,
    cmd_tx: mpsc::UnboundedSender<AlarmCommand>,
    cmd_rx: mpsc::UnboundedReceiver<AlarmCommand>,
}

impl Drop for AlarmManager {
    fn drop(&mut self) {
        for (_, handle) in self.ringing.values() {
            handle.abort();
        }
    }
}

#[derive(Clone)]
pub struct AlarmHandle {
    cmd_tx: mpsc::UnboundedSender<AlarmCommand>,
}

impl AlarmHandle {
    pub fn add_alarm(&self, wakeup_time: DateTime<Local>) -> Uuid {
        let id = Uuid::new_v4();

        // The receiving end lives inside AlarmManager::run(); if that task has
        // already shut down there is nowhere for this alarm to go.
        let _ = self.cmd_tx.send(AlarmCommand::Add(Alarm { id, wakeup_time }));

        id
    }

    /// Removes a pending alarm, or stops one that is currently ringing.
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

    /// Suppresses ringing for `duration` from now. A later call overwrites
    /// the previous mute rather than extending it additively.
    pub fn pause(&self, duration: Duration) {
        let _ = self.cmd_tx.send(AlarmCommand::Pause(duration));
    }

    /// Immediately silences every currently-ringing alarm (dismiss), without
    /// needing to know their ids. Alarms that haven't fired yet are left
    /// scheduled.
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
    /// Uses [`LogRinger`] as a stand-in until real alarm output exists.
    pub fn new() -> Self {
        Self::with_ringer(Arc::new(LogRinger))
    }

    pub fn with_ringer(ringer: Arc<dyn Ringer>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        AlarmManager {
            queue: BinaryHeap::new(),
            ringing: HashMap::new(),
            muted_until: None,
            ringer,
            cmd_tx,
            cmd_rx,
        }
    }

    pub fn handle(&self) -> AlarmHandle {
        AlarmHandle {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    fn deadline_for(wakeup_time: DateTime<Local>) -> Instant {
        let dur = (wakeup_time - Local::now()).to_std().unwrap_or(Duration::ZERO);
        Instant::now() + dur
    }

    /// The wakeup deadline, pushed back to `muted_until` if a pause is
    /// still in effect by then.
    fn effective_deadline(&self, wakeup_time: DateTime<Local>) -> Instant {
        let deadline = Self::deadline_for(wakeup_time);

        match self.muted_until {
            Some(mute) if mute > deadline => mute,
            _ => deadline,
        }
    }

    /// Moves `alarm` from due-to-ring into the actively-ringing set,
    /// spawning the [`Ringer`]'s own future and aborting it on delete.
    fn start_ringing(&mut self, alarm: Alarm) {
        let ringer = Arc::clone(&self.ringer);
        let ringing_alarm = alarm.clone();

        let join = tokio::spawn(async move { ringer.ring(&ringing_alarm).await });

        self.ringing.insert(alarm.id, (alarm, join));
    }

    fn stop_ringing(&mut self, id: Uuid) {
        if let Some((_, handle)) = self.ringing.remove(&id) {
            handle.abort();
        }
    }

    fn handle_command(&mut self, cmd: AlarmCommand) {
        match cmd {
            AlarmCommand::Add(alarm) => self.queue.push(Reverse(alarm)),
            AlarmCommand::Delete(id) => {
                self.queue.retain(|Reverse(alarm)| alarm.id != id);
                self.stop_ringing(id);
            }
            AlarmCommand::List(reply_tx) => {
                let mut alarms: Vec<Alarm> = self
                    .queue
                    .iter()
                    .map(|Reverse(alarm)| alarm.clone())
                    .chain(self.ringing.values().map(|(alarm, _)| alarm.clone()))
                    .collect();
                alarms.sort();
                let _ = reply_tx.send(alarms);
            }
            AlarmCommand::Pause(duration) => {
                self.muted_until = Some(Instant::now() + duration);
            }
            AlarmCommand::StopAll => {
                let ringing_ids: Vec<Uuid> = self.ringing.keys().copied().collect();
                for id in ringing_ids {
                    self.stop_ringing(id);
                }
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            let Some(Reverse(next)) = self.queue.peek() else {
                // No alarms scheduled: just wait for the next command.
                match self.cmd_rx.recv().await {
                    Some(cmd) => self.handle_command(cmd),
                    None => return,
                }
                continue;
            };

            let deadline = self.effective_deadline(next.wakeup_time);

            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    // Guards against a mute that got extended in the same
                    // tick the deadline fired; the loop will recompute a
                    // fresh deadline against the new mute expiry.
                    if matches!(self.muted_until, Some(mute) if mute > Instant::now()) {
                        continue;
                    }

                    if let Some(Reverse(alarm)) = self.queue.pop() {
                        self.start_ringing(alarm);
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
