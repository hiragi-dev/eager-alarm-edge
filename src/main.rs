use std::{error::Error, path::PathBuf};

use chrono::{NaiveTime, Weekday};
use clap::Parser;
use eager_alarm_edge::{Alarm, AlarmManager, naive_time_hm};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, Transport};
use serde::{Deserialize, Serialize};
use tokio_rustls::rustls::ClientConfig;
use tracing::{error, info, warn};
use uuid::Uuid;

const COMMAND_TOPIC: &str = "eager-alarm/pi/command";
const ALARMS_TOPIC: &str = "eager-alarm/pi/alarms";
const STATUS_TOPIC: &str = "eager-alarm/pi/status";
/// 鳴動状態応答トピック（`ringing_status` コマンドへの応答）
const RINGING_STATUS_TOPIC: &str = "eager-alarm/pi/ringing_status";

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct MqttConfig {
    #[arg(long, env = "MQTT_HOST")]
    mqtt_host: String,

    #[arg(long, env = "MQTT_PORT", default_value_t = 8883)]
    mqtt_port: u16,

    #[arg(long, env = "MQTT_CLIENT_ID", default_value = "pi")]
    mqtt_client_id: String,

    #[arg(long, env = "MQTT_USERNAME")]
    mqtt_username: String,

    #[arg(long, env = "MQTT_PASSWORD")]
    mqtt_password: String,

    /// アラーム設定の保存先。未指定なら `default_alarms_file()` を使う。
    #[arg(long, env = "ALARMS_FILE")]
    alarms_file: Option<PathBuf>,
}

/// アラーム設定ファイルのデフォルトの保存場所を決める。
///
/// systemd の `StateDirectory=` が設定されていれば `$STATE_DIRECTORY`
/// （ユーザーサービスなら `~/.local/state/eager-alarm-edge` 等、
/// systemd が作成・権限設定まで行う）を最優先する。
/// systemd 抜きで動かす場合は XDG の state ディレクトリ
/// （`dirs::state_dir()`）にフォールバックする。
fn default_alarms_file() -> PathBuf {
    if let Ok(state_directory) = std::env::var("STATE_DIRECTORY") {
        if let Some(dir) = state_directory.split(':').find(|s| !s.is_empty()) {
            return PathBuf::from(dir).join("alarms.json");
        }
    }

    dirs::state_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("eager-alarm-edge")
        .join("alarms.json")
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AlarmRequest {
    Add {
        #[serde(with = "naive_time_hm")]
        time: NaiveTime,
        #[serde(default)]
        days_of_week: Vec<Weekday>,
        #[serde(default = "default_true")]
        is_enabled: bool,
    },
    Edit {
        id: Uuid,
        #[serde(with = "naive_time_hm")]
        time: NaiveTime,
        #[serde(default)]
        days_of_week: Vec<Weekday>,
        #[serde(default = "default_true")]
        is_enabled: bool,
    },
    Delete {
        id: Uuid,
    },
    List,
    /// 現在鳴動中のアラームがあるかどうかを問い合わせる。
    /// 応答は `eager-alarm/<id>/ringing_status` トピックに publish される。
    RingingStatus,
    Pause {
        duration_ms: u64,
    },
    Stop,
    Status,
}

fn default_true() -> bool {
    true
}

#[derive(Serialize)]
struct StatusReply {
    online: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    color_backtrace::install();

    dotenvy::dotenv().ok();
    let config = MqttConfig::parse();

    let mut mqttoptions =
        MqttOptions::new(&config.mqtt_client_id, &config.mqtt_host, config.mqtt_port);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));
    mqttoptions.set_credentials(&config.mqtt_username, &config.mqtt_password);

    let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
    root_cert_store.add_parsable_certificates(
        rustls_native_certs::load_native_certs().expect("could not load platform certs"),
    );

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    mqttoptions.set_transport(Transport::tls_with_config(client_config.into()));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client
        .subscribe(COMMAND_TOPIC, QoS::ExactlyOnce)
        .await
        .unwrap();

    let alarms_file = config.alarms_file.clone().unwrap_or_else(default_alarms_file);
    info!(path = %alarms_file.display(), "using alarms store");
    let mut alarm = AlarmManager::with_store(alarms_file);
    let alarm_handle = alarm.handle();

    tokio::spawn(async move { alarm.run().await });

    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                match serde_json::from_slice::<AlarmRequest>(&p.payload) {
                    Ok(AlarmRequest::Add {
                        time,
                        days_of_week,
                        is_enabled,
                    }) => {
                        let new_alarm = Alarm {
                            id: Uuid::new_v4(),
                            time,
                            days_of_week,
                            is_enabled,
                        };
                        let id = alarm_handle.add_alarm(new_alarm.clone());
                        info!(%id, %time, "added alarm");
                    }
                    Ok(AlarmRequest::Edit {
                        id,
                        time,
                        days_of_week,
                        is_enabled,
                    }) => {
                        let edit_alarm = Alarm {
                            id,
                            time,
                            days_of_week,
                            is_enabled,
                        };
                        alarm_handle.edit_alarm(edit_alarm.clone());
                        info!(%id, %time, "edited alarm");
                    }
                    Ok(AlarmRequest::Delete { id }) => {
                        alarm_handle.delete_alarm(id);
                        info!(%id, "deleted alarm");
                    }
                    Ok(AlarmRequest::List) => {
                        let alarms = alarm_handle.list_alarms().await;
                        let payload = serde_json::to_vec(&alarms)?;
                        client
                            .publish(ALARMS_TOPIC, QoS::AtLeastOnce, false, payload)
                            .await?;

                        info!(count = alarms.len(), "listed alarms");
                    }
                    Ok(AlarmRequest::RingingStatus) => {
                        let status = alarm_handle.get_ringing_status().await;
                        let payload = serde_json::to_vec(&status)?;
                        client
                            .publish(RINGING_STATUS_TOPIC, QoS::AtLeastOnce, false, payload)
                            .await?;

                        info!(
                            is_ringing = status.is_ringing,
                            count = status.ringing_ids.len(),
                            "replied to ringing_status"
                        );
                    }
                    Ok(AlarmRequest::Pause { duration_ms }) => {
                        alarm_handle.pause(std::time::Duration::from_millis(duration_ms));
                        info!(duration_ms, "paused alarm output");
                    }
                    Ok(AlarmRequest::Stop) => {
                        alarm_handle.stop_all();
                        info!("stopped all ringing alarms");
                    }
                    Ok(AlarmRequest::Status) => {
                        let payload = serde_json::to_vec(&StatusReply { online: true })?;
                        client
                            .publish(STATUS_TOPIC, QoS::AtLeastOnce, false, payload)
                            .await?;

                        info!("replied to status check");
                    }
                    Err(e) => {
                        let payload_str = String::from_utf8_lossy(&p.payload);
                        warn!(error = %e, payload = %payload_str, "invalid command payload");
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = ?e, "mqtt event loop error");
                return Ok(());
            }
        }
    }
}
