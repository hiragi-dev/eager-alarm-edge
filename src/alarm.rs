use std::{cmp::Reverse, collections::BinaryHeap, time::Duration};

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
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

enum AlarmCommand {
    Add(Alarm),
    Delete(Uuid),
    List(oneshot::Sender<Vec<Alarm>>),
}

pub struct AlarmManager {
    queue: BinaryHeap<Reverse<Alarm>>,
    cmd_tx: mpsc::UnboundedSender<AlarmCommand>,
    cmd_rx: mpsc::UnboundedReceiver<AlarmCommand>,
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
}

impl AlarmManager {
    pub fn new() -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        AlarmManager {
            queue: BinaryHeap::new(),
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

    fn ring(alarm: &Alarm) {
        tracing::trace!(id = %alarm.id, wakeup_time = %alarm.wakeup_time, "alarm ringing");
    }

    fn handle_command(&mut self, cmd: AlarmCommand) {
        match cmd {
            AlarmCommand::Add(alarm) => self.queue.push(Reverse(alarm)),
            AlarmCommand::Delete(id) => self.queue.retain(|Reverse(alarm)| alarm.id != id),
            AlarmCommand::List(reply_tx) => {
                let mut alarms: Vec<Alarm> =
                    self.queue.iter().map(|Reverse(alarm)| alarm.clone()).collect();
                alarms.sort();
                let _ = reply_tx.send(alarms);
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

            let deadline = Self::deadline_for(next.wakeup_time);

            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    if let Some(Reverse(alarm)) = self.queue.pop() {
                        Self::ring(&alarm);
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
