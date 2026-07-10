# eager-alarm-edge

Alarm scheduler that runs on the edge device (e.g. a Raspberry Pi) and is controlled remotely over MQTT.

## Configuration

MQTT connection settings are read from CLI flags, environment variables, or a `.env` file (checked in that
order of precedence; `.env` is loaded into the environment on startup and is git-ignored).

| CLI flag            | Env var           | Default | Required |
| -------------------- | ------------------ | ------- | -------- |
| `--mqtt-host`         | `MQTT_HOST`         | -       | yes      |
| `--mqtt-port`         | `MQTT_PORT`         | `8883`  | no       |
| `--mqtt-client-id`    | `MQTT_CLIENT_ID`    | `pi`    | no       |
| `--mqtt-username`     | `MQTT_USERNAME`     | -       | yes      |
| `--mqtt-password`     | `MQTT_PASSWORD`     | -       | yes      |

Example `.env`:

```
MQTT_HOST=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.s1.eu.hivemq.cloud
MQTT_USERNAME=...
MQTT_PASSWORD=...
```

Or via CLI flags, which override the environment:

```
cargo run -- --mqtt-host xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.s1.eu.hivemq.cloud \
  --mqtt-username ... --mqtt-password ...
```

## MQTT API

The device subscribes to `eager-alarm/pi/command`. Every message on that topic is a JSON object with a
`type` field selecting the command.

### Add an alarm

```
{"type": "add", "wakeup_time": "2026-07-10T13:31:30+09:00"}
```

`wakeup_time` accepts either an RFC3339 timestamp (with timezone offset, as above) or a bare
`"YYYY-MM-DD HH:MM:SS"` string, which is interpreted as local time on the device.

```
mosquitto_pub -t eager-alarm/pi/command -m '{"type":"add","wakeup_time":"2026-07-10 13:31:30"}'
```

### Delete an alarm

```
{"type": "delete", "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6"}
```

`id` is the alarm's UUID, as returned by `list`.

```
mosquitto_pub -t eager-alarm/pi/command -m '{"type":"delete","id":"3fa85f64-5717-4562-b3fc-2c963f66afa6"}'
```

### List alarms

```
{"type": "list"}
```

```
mosquitto_pub -t eager-alarm/pi/command -m '{"type":"list"}'
```

The device replies on `eager-alarm/pi/alarms` with a JSON array of the currently scheduled alarms, soonest
first:

```
mosquitto_sub -t eager-alarm/pi/alarms
```

```json
[
  { "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6", "wakeup_time": "2026-07-10T13:31:30+09:00" },
  { "id": "9c858901-8a57-4791-81fe-4c455b099bc9", "wakeup_time": "2026-07-10T20:00:00+09:00" }
]
```
