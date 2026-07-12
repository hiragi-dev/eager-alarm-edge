# Eager Alarm MQTT API Specification (v2)

This document describes the updated MQTT API payload schema for the eager-alarm system. It enables repeating alarms by specifying a time and days of the week, instead of a one-time exact datetime.

**Topics:**
- Commands (Frontend -> Edge): `eager-alarm/{device_id}/command`
- Alarms List (Edge -> Frontend): `eager-alarm/{device_id}/alarms`
- Status (Edge -> Frontend): `eager-alarm/{device_id}/status`
- Ringing Status (Edge -> Frontend): `eager-alarm/{device_id}/ringing_status`

## Alarm Object Model

An alarm is represented by the following JSON structure:

```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "time": "08:00",
  "days_of_week": ["Mon", "Tue", "Wed", "Thu", "Fri"],
  "is_enabled": true,
  "stop_method_id": "geo:office"
}
```

- `id` (String): UUID of the alarm.
- `time` (String): Time in `HH:MM` format (24-hour clock).
- `days_of_week` (Array of Strings): Days when the alarm should ring. Valid values: `"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"`. Empty array means the alarm will never ring automatically.
- `is_enabled` (Boolean): Master toggle for the alarm. If false, the alarm is skipped even if the day and time match.
- `stop_method_id` (String or `null`): Opaque identifier of a location-based stop method managed by the app (frontend). The edge does not interpret or validate this value in any way — it is stored as given and returned unchanged. `null` if none is set, including for alarms created before this field existed.

---

## Command Messages (`eager-alarm/{device_id}/command`)

All command messages must be sent as a JSON object with a `type` field.

### 1. Add Alarm
Creates a new alarm. The backend will generate and assign a new UUID.

```json
{
  "type": "add",
  "time": "07:30",
  "days_of_week": ["Sat", "Sun"],
  "is_enabled": true,
  "stop_method_id": "geo:office"
}
```

`stop_method_id` is optional; if omitted, it is stored as `null`.

### 2. Edit Alarm
Modifies an existing alarm.

```json
{
  "type": "edit",
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "time": "08:15",
  "days_of_week": ["Mon", "Wed", "Fri"],
  "is_enabled": false,
  "stop_method_id": "geo:office"
}
```

`stop_method_id` is optional; if omitted, the alarm's value is overwritten with `null` (edit replaces the full alarm, it does not merge fields).

### 3. Delete Alarm
Deletes an alarm by its ID.

```json
{
  "type": "delete",
  "id": "123e4567-e89b-12d3-a456-426614174000"
}
```

### 4. List Alarms
Requests the edge device to publish the current list of alarms. The response will be sent to the `alarms` topic.

```json
{
  "type": "list"
}
```

### 5. Pause Ringing
Pauses an actively ringing alarm for a specified duration in milliseconds. The alarm will resume ringing automatically after the duration expires unless another pause is sent or it is stopped.

```json
{
  "type": "pause",
  "duration_ms": 10000
}
```

### 6. Stop Ringing
Stops all currently ringing alarms. **Note for v2:** The alarm is not deleted. It remains scheduled for the next applicable day in `days_of_week`.

```json
{
  "type": "stop"
}
```

### 7. Status Check
Requests the edge device to broadcast its online status.

```json
{
  "type": "status"
}
```

### 8. Ringing Status Check
Requests whether any alarm is currently ringing. The response is sent to the `ringing_status` topic.

Use this to synchronize UI state on app launch, after reconnecting to the broker, or to poll the alarm state at any time.

```json
{
  "type": "ringing_status"
}
```

---

## Response Messages

### 1. Alarms List (`eager-alarm/{device_id}/alarms`)
Triggered by the `list` command (and usually automatically broadcasted when an alarm is added/edited/deleted). The payload is a JSON array of Alarm objects.

```json
[
  {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "time": "08:00",
    "days_of_week": ["Mon", "Tue", "Wed", "Thu", "Fri"],
    "is_enabled": true,
    "stop_method_id": "geo:office"
  }
]
```

### 2. Status (`eager-alarm/{device_id}/status`)
Triggered by the `status` command.

```json
{
  "online": true
}
```

### 3. Ringing Status (`eager-alarm/{device_id}/ringing_status`)
Triggered by the `ringing_status` command. Indicates whether any alarm is currently ringing and provides a list of ringing alarm IDs.

```json
{
  "is_ringing": true,
  "ringing_ids": [
    "123e4567-e89b-12d3-a456-426614174000"
  ]
}
```

- `is_ringing` (Boolean): `true` if at least one alarm is currently ringing.
- `ringing_ids` (Array of Strings): UUIDs of all currently ringing alarms. Empty array if nothing is ringing.

> **Typical use case:** Subscribe to `ringing_status` on app startup and send a `ringing_status` command immediately after connecting. If `is_ringing` is `true`, show the dismiss UI without waiting for an alarm to fire.
