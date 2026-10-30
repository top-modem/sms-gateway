# More Features Design: Machine Types, Multi-Profile eSIM, Multi-SIM

Status: **Partially implemented.** Multi-profile eSIM management, image phone OCR, machine-type labels, and persisted Multi-SIM slot labels are implemented. Dedicated Multi-SIM switching controls remain planned.

## 0. Confirmed Decisions

| # | Decision |
|---|---|
| 1 | `ath?` returns `ERROR` on a normal machine. |
| 2 | An eSIM machine can have only **one enabled profile** at a time. |
| 3 | The dashboard shows only the active Multi-SIM card, with its current slot number. |
| 4 | The dashboard shows the active eSIM profile nickname. |
| 5 | Normal-machine rows in the SIM dashboard stay exactly as they are today. |
| 6 | Bank/slot mapping and protected-slot labels are not included. |
| 7 | In eSIM mode the COM port does not answer normal AT commands. |
| 8 | An eUICC holds at most **20 profiles**. |
| 9 | A Multi-SIM port stays on the selected slot to receive and send SMS; no automatic rotation. |
| 10 | No AT command reads the current Multi-SIM slot, so the current slot is stored in the database. |
| 11 | After reboot or `ath0`, a Multi-SIM machine returns to **slot 1**. |
| 12 | Configured phone numbers are stored in existing SIM card records by ICCID. |

Hardware check on 2026-10-30: `ath?` on COM1 and COM2 of the connected eSIM machine returned `\r\nESIM\r\n`. `ath?` on COM1 of the Multi-SIM machine returned `\r\nMSIM` with no `OK`.

Multi-SIM slot test on COM1, 2026-10-30:

| Command | Reply | SIM after 15 s | ICCID |
|---|---|---|---|
| `ath1` | `OK` in ~380 ms | `READY` | `89860443231880603155` |
| `ath2` | `OK` in ~380 ms | `READY` | `898604762620C0779112` |
| `ath3` | `OK` in ~380 ms | `READY` | `89860408091880047908` |
| `ath4` | `OK` in ~380 ms | `READY` | `8944200203918040386F` |

Each slot exposed a different ICCID and IMSI. Network registration stayed at `CREG 0,2` with no signal for the test SIMs, so registration time was not measured. COM1 was returned to slot 1.

## 1. Goals

1. Detect and show three machine types: **Normal**, **eSIM**, and **Multi-SIM (MSIM)**.
2. Replace the current single-profile eSIM workflow with a multi-profile manager.
3. Support Multi-SIM machines: four SIM slots per port, switched with `ath1`–`ath4`.
4. Show eSIM and Multi-SIM information in the SIM dashboard without changing normal-machine rows.
5. Keep normal SMS receiving stable while ports switch modes or slots.

Out of scope:

- Remote eSIM worker proxy used by `D:\sms-gateway\sms-gateway`.
- SIM-bank mapping and protected-slot data, such as `B1:2 -> C2:0`.
- Automatic SIM rental or customer assignment.

## 2. Hardware Rules

### 2.1 Machine type detection

All types are detected per COM port by sending `ath?`.

| Reply contains | Machine type | Notes |
|---|---|---|
| `ESIM` | eSIM | Current code already uses this rule |
| `MSIM` | Multi-SIM | From the multi-SIM interface document |
| `ERROR` | Normal | Confirmed |
| Timeout or unexpected reply | Unknown | Retry later; do not cache |

`ESIM` and `MSIM` results are cached and saved in the database. A `Normal` result is re-checked after reconnect because a disconnected or busy port can also fail the command.

`ath?` replies `ESIM` or `MSIM` without a trailing `OK`. The serial response reader must treat both as complete replies. Today it recognizes only `ESIM`, so `MSIM` waits for timeout, blocks the port's command queue, and can make the app mark the port unavailable.

A port in eSIM mode does not answer `ath?`, so its type comes from the saved value instead of a new probe.

### 2.2 eSIM machine

| Action | AT sequence | Result |
|---|---|---|
| Enter eSIM mode | `atl6`, then `ath7` | Smart-card reader appears; port cannot receive SMS |
| Exit eSIM mode | `atl7`, then `ath6` | Port returns to normal mode; ICCID becomes enabled profile |
| Recover | `ath0` | Factory default; operation can restart |

Constraints:

- **Only one eSIM port per machine can be in eSIM mode at a time.** The existing machine-wide gate stays.
- **Only one profile per eUICC can be enabled.** Enabling a profile first disables the currently enabled profile.
- **Maximum 20 profiles per eUICC.** Download is blocked in the UI and API when 20 profiles already exist.
- In eSIM mode the port does not answer normal AT commands. SMS, signal, SIM checks, and `ath?` are skipped until exit.

### 2.3 Multi-SIM machine

| Action | AT command | Result |
|---|---|---|
| Select slot 1–4 | `ath1` … `ath4` | Replies `OK`; active SIM changes; registration takes about 10–15 seconds |
| Empty slot | Selection reply contains `NO SIM` | Slot is marked empty |
| Recover | `ath0` | Factory default |
| Detect type | `ath?` | Reply contains `MSIM` |

Only one slot per port is active. Slot switching changes the SIM on that COM port, so existing SIM-swap detection must treat it as an intentional change.

Slot state rules:

- No AT command reads the current slot. The app stores the last successful `athN` in the database.
- After reboot or `ath0`, the machine returns to **slot 1**. The app sets the stored slot to `1` when it sends `ath0` or detects the port disconnected and reconnected.
- The port stays on the selected slot for SMS receive and send. There is no automatic slot rotation.

## 3. Current Gaps

### 3.1 eSIM

The current implementation can list multiple profiles, but most workflows assume one:

- `current_iccid()` uses the first profile.
- Batch enable, disable, and delete operate on the current profile.
- Batch download can delete all profiles before downloading.
- `esim_batch_replace_existing` defaults to `true`.
- The UI places session control, profile actions, download, and batch tools in one page with limited per-profile context.

### 3.2 Dashboard

- `/api/sims/info` does not return machine type.
- eSIM rows do not show eSIM mode or the active profile nickname.
- Multi-SIM rows cannot show the active slot.

### 3.3 Multi-SIM

There is no slot model, API, or UI. A manual slot switch would currently look like a SIM swap to the recheck worker.

## 4. Proposed Data Model

### 4.1 Port hardware state

New table: `port_hardware`

| Column | Type | Description |
|---|---|---|
| `com_port` | TEXT PK | Example: `COM3` |
| `machine_type` | TEXT | `normal`, `esim`, `msim`, `unknown` |
| `type_reply` | TEXT | Last raw `ath?` reply |
| `active_slot` | INTEGER NULL | Multi-SIM slot from the last successful switch; `1` after reboot or reset |
| `detected_at` | TEXT | Last successful detection |
| `updated_at` | TEXT | Last update |

### 4.2 Multi-SIM slots

New table: `msim_slots`

| Column | Type | Description |
|---|---|---|
| `com_port` | TEXT | Port |
| `slot` | INTEGER | 1–4 |
| `state` | TEXT | `active`, `standby`, `empty`, `error`, `unknown` |
| `iccid` | TEXT NULL | Last ICCID read from slot |
| `imsi` | TEXT NULL | Last IMSI read from slot |
| `last_checked_at` | TEXT NULL | Last probe |
| `last_error` | TEXT NULL | Last switch or read error |

Primary key: `(com_port, slot)`.

### 4.3 eSIM profiles

Keep `esim_profiles` and use it as the last known profile inventory:

- Store all profiles per `(com_port, iccid)`.
- Store EID once read.
- Mark profiles not returned by the latest `lpac profile list` as removed or delete them from the cache.
- Show cached profiles outside a session, but label them **Last read** with a timestamp.

### 4.4 Configured MSISDN

Use the existing `sim_cards.phone_number` keyed by ICCID. No new eSIM phone-number table is needed.

An eSIM profile ICCID becomes the SIM ICCID after it is enabled, so the same phone number follows the profile across eSIM and normal SIM views. If a phone number is entered before download, it is saved after the downloaded ICCID is known.

## 5. Backend Design

### 5.1 Machine type service

Add a small hardware-detection layer used by modem initialization and port refresh:

- Probe `ath?` after a modem is responsive.
- Cache positive `esim` and `msim` results.
- Save the result to `port_hardware`.
- Expose type through modem snapshots and APIs.

### 5.2 `/api/sims/info` additions

Add these optional fields per port:

```json
{
  "machine_type": "normal | esim | msim | unknown",
  "esim_mode": false,
  "esim_active_nickname": "CMCC Main",
  "esim_active_iccid": "8986...",
  "msim_active_slot": 2,
  "msim_slot_count": 4
}
```

Rules:

- Normal ports only add `machine_type: "normal"`; the dashboard ignores it.
- `esim_active_nickname` comes from the cached enabled profile. If the nickname is empty, use profile name, then provider name, then the last 6 ICCID digits.
- `msim_active_slot` comes from the stored slot in `port_hardware`.
- The handler continues skipping live AT queries for ports in eSIM mode.

### 5.3 eSIM API changes

Keep current routes for compatibility and add profile-explicit routes:

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/api/esim/ports` | Ports, type, mode, EID, profile count, last operation |
| `GET` | `/api/esim/{com}/inventory` | Cached EID and profiles, without entering eSIM mode |
| `POST` | `/api/esim/{com}/inventory/refresh` | Enter, read chip and profiles, exit, update cache |
| `POST` | `/api/esim/{com}/profiles/{iccid}/enable` | Enable one profile |
| `POST` | `/api/esim/{com}/profiles/{iccid}/disable` | Disable one profile |
| `DELETE` | `/api/esim/{com}/profiles/{iccid}` | Disable if needed, then delete |
| `PUT` | `/api/esim/{com}/profiles/{iccid}/nickname` | Rename profile |
| `PUT` | `/api/esim/profiles/{iccid}/msisdn` | Save configured phone number |

Download behavior changes:

- Default action is **add profile**, not replace.
- `replace_existing` remains available but defaults to `false` and requires explicit confirmation.
- `enable_after_download` disables the currently enabled profile, then enables the new profile.
- Downloaded ICCID is resolved by comparing profile lists before and after download.
- Enable always follows the one-enabled-profile rule: disable current, enable target, read the profile list back to confirm.

### 5.4 eSIM batch changes

Batch items should target profiles where needed:

```json
{
  "op": "download | enable | disable | delete | refresh",
  "items": [
    { "com_port": "COM3", "iccid": "8986...", "activation_code": "LPA:1$..." }
  ],
  "options": {
    "enable_after_download": true,
    "replace_existing": false,
    "stop_on_error": false
  }
}
```

Rules:

- Download needs an activation code.
- Enable, disable, and delete need an ICCID, except for “all profiles on selected ports” operations.
- Bulk delete requires typed confirmation.
- Every item always exits eSIM mode; on failure it attempts `ath0` recovery.

### 5.5 Multi-SIM API

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/api/msim/ports` | Multi-SIM ports and slot summary |
| `GET` | `/api/msim/{com}/slots` | Slot details |
| `POST` | `/api/msim/{com}/slots/{slot}/select` | Switch to slot and wait for registration |
| `POST` | `/api/msim/{com}/scan` | Probe slots 1–4 and restore original slot |
| `POST` | `/api/msim/{com}/reset` | Send `ath0` |
| `POST` | `/api/msim/batch` | Switch or scan multiple ports |

Slot switch flow:

1. Lock the port for slot switching.
2. Pause SMS reads and SIM recheck for that port.
3. Send `athN`.
4. If reply contains `NO SIM`, mark slot empty and restore previous slot when needed.
5. Wait up to a configurable timeout for `CPIN READY` and a readable ICCID. Network registration is reported but does not block or fail the switch.
6. Update the port’s SIM identity and slot table.
7. Resume workers.

Proposed configuration:

```toml
msim_enabled = true
msim_switch_timeout_secs = 30
msim_restore_after_scan = true
```

## 6. UI Design

The attached screenshot is used as a reference only.

### 6.1 eSIM Manager

```text
┌──────────────────────────────────────────────────────────────────────┐
│ eSIM Manager                                      [Refresh all]      │
│ COM3 is in eSIM mode. Only one port can be managed at a time. [Exit] │
├──────────────────────────────────────────────────────────────────────┤
│ Search [port / EID / ICCID]  Type [eSIM]  Mode [All]  Profiles [All] │
│ Selected 2 ports: [Refresh] [Download] [Enable] [Disable] [Delete]  │
├────┬──────┬────────┬────────┬────────────┬──────────┬──────────────┤
│ ☐  │ Port │ Mode   │ EID    │ Profiles   │ Active   │ Actions      │
│ ☑  │ COM3 │ eSIM   │ 8904…  │ 3 / 1 on   │ 8986…    │ Manage Reset │
│ ☐  │ COM4 │ Normal │ 8904…  │ 2 / 1 on   │ 8944…    │ Manage Reset │
└────┴──────┴────────┴────────┴────────────┴──────────┴──────────────┘
```

Clicking **Manage** opens a right-side detail drawer:

```text
COM3 · eSIM mode                                      [Refresh] [Exit]
EID: 8904...                      Last read: 2026-10-30 10:06

Profiles
┌──────────────┬───────────────┬──────────┬────────────┬──────────────┐
│ ICCID        │ Name/Provider │ State    │ Phone      │ Actions      │
│ 8986...      │ CMCC          │ Enabled  │ 138...     │ Disable ...  │
│ 8944...      │ Vodafone      │ Disabled │ —          │ Enable  ...  │
└──────────────┴───────────────┴──────────┴────────────┴──────────────┘

Add profile
Activation code: [LPA:1$...]
Confirmation code: [optional]
Configured phone number: [optional]
☐ Enable after download
[Download]

Advanced: notifications, replace-all download, operation history
```

Profile actions:

- **Enable** and **Disable**.
- **Rename**.
- **Edit phone number**.
- **Delete**, with confirmation including ICCID and name.

### 6.2 eSIM batch workflow

The batch download dialog keeps the existing source support:

- Server folder scan.
- File upload.
- Paste LPA codes.

New pairing table:

| Port | Activation code | Phone number | Enable after download | Status |
|---|---|---|---|---|

Profile batch operations use a selector:

- Selected profiles.
- Enabled profile on each selected port.
- All disabled profiles on selected ports.
- All profiles on selected ports, with typed confirmation.

### 6.3 Multi-SIM Manager

```text
Multi-SIM Manager
Selected 2 ports: [Scan slots] [Switch to slot...] [Reset]

┌────┬──────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ ☐  │ Port │ Slot 1   │ Slot 2   │ Slot 3   │ Slot 4   │ Actions  │
│ ☑  │ COM8 │ Standby  │ Active   │ Empty    │ Standby  │ Manage   │
│    │      │ 8986…    │ 8944…    │ NO SIM   │ 8985…    │ Reset    │
└────┴──────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```

The detail drawer shows ICCID, IMSI, phone number, last checked time, and **Use this slot** for each slot. A switch shows progress until registration completes or times out.

### 6.4 SIM Dashboard

No new column is added. Normal and unknown rows remain unchanged.

Only eSIM and Multi-SIM rows get a compact label under the COM port:

```text
COM Port
COM1                         normal machine: unchanged
COM3
[eSIM] CMCC Main             active profile nickname
COM8
[MSIM] Slot 2                current active slot
```

| Machine | Label |
|---|---|
| Normal | None |
| eSIM, normal mode | `eSIM` badge and active profile nickname |
| eSIM, eSIM mode | `eSIM` badge and `eSIM mode` status |
| eSIM, no enabled profile | `eSIM` badge and `No active profile` |
| Multi-SIM | `MSIM` badge and `Slot N` |
| Multi-SIM switching | `MSIM` badge and `Switching to slot N` |

The dashboard shows only the active Multi-SIM card. Inactive slots appear only in the Multi-SIM manager.

Rows in eSIM mode or during a slot switch must not show stale signal or network values as current.

## 7. Worker and Concurrency Rules

- eSIM sessions remain machine-wide single-port sessions.
- Multi-SIM switching is locked per port.
- A port cannot be in eSIM mode and Multi-SIM switching at the same time.
- SMS reading, recheck, and SIM-swap demotion skip ports that are in eSIM mode or slot switching.
- Expected ICCID changes after eSIM enable or Multi-SIM switch update the port identity instead of being treated as an unexpected swap.
- All long-running operations write to an operation log.

## 8. Implementation Phases

### Phase 1: Machine type foundation

- Accept `MSIM` as a complete `ath?` reply in the serial response reader.
- Detect `normal`, `esim`, `msim`, and `unknown`.
- Persist hardware type.
- Add type fields to `/api/sims/info`.
- Add eSIM and Multi-SIM labels to the dashboard; leave normal rows unchanged.

### Phase 2: Multi-profile eSIM backend

- Add profile-explicit APIs.
- Remove single-profile assumptions from batch actions.
- Change download default from replace to add.
- Cache and expose profile inventory.

### Phase 3: New eSIM UI

- Port table and detail drawer.
- Multi-profile table and profile actions.
- Batch download pairing and profile-level batch operations.

### Phase 4: Multi-SIM backend and UI

- Slot switching, scanning, recovery, and slot cache.
- Worker pause/resume and identity handling.
- Multi-SIM manager page and dashboard slot display.

### Phase 5: Hardware testing

- Machine type detection on all three hardware types.
- eSIM: download two or more profiles, switch enabled profile, delete disabled profile, recover with `ath0`.
- Multi-SIM: switch slots 1–4, empty slot handling, restart while switched, SMS receive after switch.

## 9. Open Questions

None at the moment.

## 10. Hardware Test Plan

| Machine | Status | Tests |
|---|---|---|
| eSIM | Connected now | `ath?` confirmed; next: multi-profile download, enable/disable, delete, 20-profile limit, exit and SMS receive |
| Multi-SIM | Connected | `ath?` confirmed `MSIM`; `ath1`–`ath4` confirmed; next: `NO SIM`, reboot to slot 1, SMS receive/send on a registered SIM |
| Normal | Later | `ath?` returns `ERROR`; dashboard unchanged |
