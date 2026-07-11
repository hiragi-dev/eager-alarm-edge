//! Example of how to configure rumqttc to connect to a server using TLS and authentication.
use std::error::Error;

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use clap::Parser;
use eager_alarm_edge::AlarmManager;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, Transport};
use serde::{Deserialize, Serialize};
use tokio_rustls::rustls::ClientConfig;
use tracing::{error, info, warn};
use uuid::Uuid;

const COMMAND_TOPIC: &str = "eager-alarm/pi/command";
const ALARMS_TOPIC: &str = "eager-alarm/pi/alarms";
const STATUS_TOPIC: &str = "eager-alarm/pi/status";

/// MQTT connection settings. Each field can be set via a CLI flag, an
/// environment variable of the same name, or a `.env` file (loaded on
/// startup) providing that variable.
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct MqttConfig {
    /// MQTT broker hostname
    #[arg(long, env = "MQTT_HOST")]
    mqtt_host: String,

    /// MQTT broker port
    #[arg(long, env = "MQTT_PORT", default_value_t = 8883)]
    mqtt_port: u16,

    /// Client id this device identifies itself as
    #[arg(long, env = "MQTT_CLIENT_ID", default_value = "pi")]
    mqtt_client_id: String,

    /// MQTT username
    #[arg(long, env = "MQTT_USERNAME")]
    mqtt_username: String,

    /// MQTT password
    #[arg(long, env = "MQTT_PASSWORD")]
    mqtt_password: String,
}

/// Payload expected on [`COMMAND_TOPIC`]: `{"type":"add","wakeup_time":"..."}`,
/// `{"type":"delete","id":"..."}`, `{"type":"list"}`,
/// `{"type":"pause","duration_ms":5000}`, `{"type":"stop"}`, or `{"type":"status"}`.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AlarmRequest {
    Add {
        #[serde(deserialize_with = "deserialize_wakeup_time")]
        wakeup_time: DateTime<Local>,
    },
    Delete {
        id: Uuid,
    },
    List,
    Pause {
        duration_ms: u64,
    },
    Stop,
    Status,
}

#[derive(Serialize)]
struct StatusReply {
    online: bool,
}

/// Accepts RFC3339 (`2026-07-10T13:31:30+09:00`) as well as a bare
/// `"YYYY-MM-DD HH:MM:SS"` string, which is interpreted as local time.
fn deserialize_wakeup_time<'de, D>(deserializer: D) -> Result<DateTime<Local>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Ok(dt.with_timezone(&Local));
    }

    let naive =
        NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").map_err(serde::de::Error::custom)?;

    Local
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| serde::de::Error::custom("ambiguous or invalid local time"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    color_backtrace::install();

    // Missing .env is fine: real deployments may set the environment directly.
    dotenvy::dotenv().ok();
    let config = MqttConfig::parse();

    let mut mqttoptions =
        MqttOptions::new(&config.mqtt_client_id, &config.mqtt_host, config.mqtt_port);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));
    mqttoptions.set_credentials(&config.mqtt_username, &config.mqtt_password);

    // Use rustls-native-certs to load root certificates from the operating system.
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

    let mut alarm = AlarmManager::new();
    let alarm_handle = alarm.handle();

    tokio::spawn(async move { alarm.run().await });

    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                match serde_json::from_slice::<AlarmRequest>(&p.payload) {
                    Ok(AlarmRequest::Add { wakeup_time }) => {
                        let id = alarm_handle.add_alarm(wakeup_time);
                        info!(%id, %wakeup_time, "added alarm");
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
                    Err(e) => warn!(error = %e, "invalid command payload"),
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
