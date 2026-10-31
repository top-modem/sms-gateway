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
| Select slot 1–4 | `ath1` … `ath4` | Replies `OK`; wait separately for a stable SIM identity |
| Empty slot | Confirmed SIM-not-inserted result after settling | A timeout or generic CME error alone does not prove an empty slot |
| Recover | `ath0` | Factory default |
| Detect type | `ath?` | Reply contains `MSIM` |

Only one slot per port is active. Slot switching changes the SIM on that COM port, so existing SIM-swap detection must treat it as an intentional change.

Slot state rules:

- No AT command reads the current slot. The app stores the last successful `athN` in the database.
- After a confirmed hardware reboot or acknowledged `ath0`, the machine returns to **slot 1**. An ordinary serial reconnect does not prove a hardware reboot.
- After an app restart, reconnect, or uncertain command outcome, display the saved selection as **Last known**, not verified. Do not silently switch to slot 1 merely to discover the current selection.
- Selecting a slot and reading its ICCID confirms the current mapping; an old cached ICCID alone cannot prove which physical slot is selected.
- Network registration is separate from slot selection. COM1 slot 2 previously needed about two minutes to register; do not use registration as the discovery completion condition.
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

Machine classification, acknowledged slot-command persistence, and the dashboard slot label are implemented. The four-slot inventory, dedicated management API, coordinated switching, and manager drawer remain planned.

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

`active_slot` is the last acknowledged selection. Track selection verification separately for the current connection; it starts unverified after process start or reconnect and is not restored to verified merely by loading a database row.

### 4.2 Multi-SIM slots

Proposed new table: `msim_slots`. Store four physical positions per Multi-SIM port, including unread and empty positions.

| Column | Type | Description |
|---|---|---|
| `com_port` | TEXT | Port |
| `slot` | INTEGER | 1–4 |
| `state` | TEXT | `unknown`, `present`, `empty`, `unreadable`; selection is separate |
| `iccid` | TEXT NULL | Last verified card association; clear only after a confirmed empty result or verified replacement |
| `imsi` | TEXT NULL | Last verified IMSI, when readable |
| `last_checked_at` | TEXT NULL | Last attempted read |
| `identity_verified_at` | TEXT NULL | Last successful identity read |
| `last_error` | TEXT NULL | Last switch or read error |

Primary key: `(com_port, slot)`; enforce `slot BETWEEN 1 AND 4` and index ICCID for lookup.

Identity and freshness rules:

- `(com_port, slot)` identifies a physical position. ICCID identifies its card. Phone number is neither a slot identifier nor a unique key.
- Keep ICCIDs as strings, including leading zeros and modem-specific hexadecimal/padding characters. Reuse existing SIM-record matching; do not numerically convert IDs or rewrite existing primary keys during this feature.
- Active/standby comes from the current port selection, not the slot's presence state. API responses also expose `selection_verified` and per-slot cached/read/error metadata.
- Save each verified slot result immediately. A later slot failure must not discard earlier successful reads.
- On a verified replacement, attach the new ICCID to this position but retain the old card's SIM record, phone number, and message history. Never copy the previous occupant's number to the replacement.
- On timeout or USB loss, retain prior identity as **Last known** and record the failed attempt. Never present it as a fresh read.
- Port names are current locations, not permanent hardware identities. After hardware replacement or COM reassignment, old mappings remain unverified until explicitly read again.

### 4.3 eSIM profiles

Keep `esim_profiles` and use it as the last known profile inventory:

- Store all profiles per `(com_port, iccid)`.
- Store EID once read.
- Mark profiles not returned by the latest `lpac profile list` as removed or delete them from the cache.
- Show cached profiles outside a session, but label them **Last read** with a timestamp.

### 4.4 Configured MSISDN

Use the existing `sim_cards.phone_number` keyed by ICCID. No new eSIM phone-number table is needed.

An eSIM profile ICCID becomes the SIM ICCID after it is enabled, so the same phone number follows the profile across eSIM and normal SIM views. If a phone number is entered before download, it is saved after the downloaded ICCID is known.

For Multi-SIM discovery:

- Read MSISDN with the existing `get_phone_number()` / `AT+CNUM` path only after the slot identity is stable. A blank or unsupported CNUM result is normal; show **Unknown number**, not a failed slot.
- Reuse a saved number for the same ICCID. Backfill an empty number from CNUM, but never overwrite a configured nonempty number with an empty or conflicting modem response.
- Add number provenance to the existing SIM record if needed: `manual`, `sim_read`, or `legacy`. Do not invent a provenance value for existing records.
- The drawer's **Number** action edits the ICCID-linked database record, including for inactive cards. It does not select the slot or call `AT+CPBS` / `AT+CPBW`.
- Multi-SIM dashboard number editing must use the same local-only behavior; update the shared phone form/handler as needed so it cannot accidentally take the physical-SIM phonebook-write path.
- An edit carries `expected_iccid`; reject it if the saved slot association changed. Normal-machine phonebook-writing behavior remains unchanged.
- Do not infer a number from ICCID, IMSI, slot number, or operator, and do not send SMS, USSD, or calls to discover it automatically.

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

Implemented routes reuse the existing Basic authentication and response envelope.

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/api/msim/ports` | Multi-SIM ports and slot summary |
| `GET` | `/api/msim/{com}/slots` | Cached four-slot inventory, number provenance, selection confidence, and current operation; no slot switching |
| `POST` | `/api/msim/{com}/scan` | Sequentially read requested slots, default 1–4, then restore the agreed slot |
| `POST` | `/api/msim/{com}/slots/{slot}/select` | Select a physical slot, verify identity, and update the active modem |
| `PUT` | `/api/msim/{com}/slots/{slot}/phone` | Save an ICCID-linked number locally, guarded by `expected_iccid` |
| `GET` | `/api/msim/operations/{id}` | Operation stage, per-slot results, restoration result, and errors |
| `POST` | `/api/msim/operations/{id}/cancel` | Request cancellation at a safe command boundary, then restore |
| `GET` | `/api/msim/operations/{id}/events` | Authenticated SSE with the initial persisted snapshot and subsequent updates |

Scan and selection return `202` with an operation ID. Duplicate request IDs reuse the existing operation; overlapping management operations return `409` (the initial release uses one global management permit). Selection and number saving require `expected_iccid`. Validate machine type, slot range, and cached identity before changing anything. Selection uses exclusive manager-owned serial access rather than requiring a READY SIM.

First-read flow:

1. Open the drawer immediately from the database, always showing four rows. Do not automatically rotate cards on page load or app startup.
2. Offer **Read all four slots**. Before starting, confirm the temporary SMS interruption and show which slot will be restored.
3. If the original selection is verified in this connection, restore that slot. Otherwise require an explicit final-slot choice; do not promise to restore an unknown selection.
4. Take the port operation lock, finish or reject incompatible in-flight work, and pause SMS/MMS polling, signal checks, SIM rechecks, and generic SIM-error reset recovery on that port.
5. Select each requested slot with `athN`. After acknowledgement, allow SIM settling and require stable identity readback before storing ICCID/IMSI. Never assign a delayed response from the previous card to the next slot; ambiguous transitions remain unverified.
6. Read CNUM best-effort, join the existing ICCID phone record, and save this slot's verified result transactionally. Do not wait for network registration or probe signal/operator on every slot.
7. Record explicit empty, PIN-required, timeout, and transport-error outcomes separately. Continue after a slot-local failure while the port remains usable; stop on transport loss. Never label a generic CME 13/50/100 or timeout as empty.
8. Restore the agreed slot in final cleanup on success, error, or cancellation. Verify its identity, refresh the manager's active ICCID/cache, then resume workers. A previously selected empty slot can be restored as an explicit empty selection.
9. Report discovery and restoration independently. If restoration fails, show **Restore failed / selection unverified**, leave identity-sensitive work blocked, and offer a deliberate slot selection; do not show overall success.

Per-slot **Read** uses the same flow with a one-slot list and restoration. Scanning is sequential within a port; initial release also limits concurrent management scans to avoid starving normal port initialization.

Slot switch flow:

1. Lock the port for slot switching.
2. Coordinate all normal workers and outstanding commands as for discovery.
3. Send `athN`; an `OK` means acknowledged selection, not yet a verified card or registered network.
4. Wait for a stable ICCID and usable SIM state within a bounded timeout. If an expected ICCID differs, report **Card changed**; do not silently select a replacement for an ICCID-targeted request.
5. Update the slot mapping, database, and manager identity together before resuming identity-sensitive work. Queued outbound messages must still match their intended ICCID.
6. Show **Selected / registering** separately from SIM readiness. Registration failure does not turn a verified slot selection into a failed selection.
7. On an unusable target or readback failure, attempt restoration of the previously verified slot and report the outcome. Never retry by silently using `ath0` or CFUN reset.

Progress reliability:

- Report stages such as selecting, reading identity, reading number, saving, restoring, done, failed, cancelled, and interrupted.
- Reuse the corrected eSIM pattern: SSE for prompt updates plus status polling about every two seconds; reconnect after stream failure and release controls only after terminal status.
- Closing the drawer does not cancel the backend operation. Reopening or reloading retrieves its current status.
- Persist operation status and final restoration outcome. On process restart mark unfinished operations interrupted, invalidate selection confidence, and reconcile before further identity-sensitive work; do not silently resume an old scan.

Reset and multi-port batch controls are later enhancements, not required for the initial four-slot manager.

Future configuration proposal (not supported by the current implementation):

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

Design revision: 2026-10-30, based on the supplied eSIM profile drawer. Implementation was subsequently authorized and resumed on 2026-10-31. Verification results and remaining limitations are recorded below.

Keep the existing compact table styling, highlighted active row, right-side drawer, and familiar icon library. Add a Multi-SIM entry alongside eSIM management in the numbers area. The main dashboard continues to show only the selected card.

```text
Multi-SIM Manager
Port   Selected       Current number   Identified   Last read   Action
COM1   Slot 1         saved number     2 / 4        timestamp   Manage
```

The count is **Identified**, not a promise that unread positions are empty. No EID, profile download, delete, or eSIM disable controls appear for physical slots.

Drawer wireframe; the mixed states below are illustrative, not current hardware readings:

```text
COM1 · Multi-SIM slots                      [Read all four] [X]
Selected: Slot 1 · saved number             Last read: timestamp

Slot     ICCID            State             Number          Actions
Slot 1   cached ICCID     Selected          saved number    Current  Number
Slot 2   cached ICCID     Standby           saved number    Use      Number
Slot 3   --               Empty             --              Read
Slot 4   --               Not read          Unknown         Read

Operation: Reading slot 2 of 4              [Cancel]
Return to: Slot 1
```

Drawer behavior:

- Use the eSIM drawer's white table, restrained borders, green selected-row tint, and neutral standby badges. Slot numbers are explicit, fixed, and always visible.
- Each slot row shows full/copyable ICCID and its saved number. Put IMSI, last successful identity read, number provenance, and read errors in a compact expandable detail row.
- Actions are **Use this slot**, **Number**, and an icon-only **Read slot** with a tooltip. The selected row shows **Current** instead of another Use button. Number editing is available for any identified card, even when inactive.
- Unknown or previously empty positions remain readable. Do not show Use as enabled for a cached-empty or unread card until it has been read; recovery can still explicitly select a physical slot through the service.
- During management, lock conflicting actions for this port and show the exact stage and slot. Other ports remain usable; cancelling still shows restoration progress before controls unlock.
- After a connection change show **Last known slot** and cached identity indicators, not a green verified-current badge. Offer explicit selection or an agreed scan return slot.
- Missing MSISDN shows **Unknown number** with Number editing available once ICCID is known. Missing phone data must not prevent use of an otherwise verified card.
- On narrow screens use a full-width drawer and four stacked slot rows, keeping slot/state/number/action visible and allowing ICCID to wrap without overlap. Do not hide slot numbers or require horizontal scrolling to reach Use.

Suggested Chinese labels follow the screenshot: `Multi-SIM 管理`, `卡槽 1` through `卡槽 4`, `读取全部`, `使用此槽`, `当前使用`, `号码`, `未读取`, `空槽`, `上次记录`, and `恢复失败`.

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

Implementation tasks below are complete in source. Runtime verification is tracked separately in section 12.

- [x] T401 [Plan:4.2,4.4] Add a migration and helpers in `src/db.rs` for four-slot associations, identity timestamps, number provenance, and persisted operation results. Preserve existing SIM records.
- [x] T402 [Plan:5.5,7] Add orchestration in `src/msim.rs` and integrate `src/modem/manager.rs` exclusive ownership, worker coordination, bounded reads, and original-slot restoration. Shut down the normal modem before opening the isolated management connection.
- [x] T403 [Plan:5.5] Add the authenticated Multi-SIM inventory, operation, selection, and local-number routes under `src/api/`, registered through `src/api/mod.rs`.
- [x] T404 [Plan:6.3] Add the manager and four-slot drawer under `frontend/src/pages/`; connect navigation in `frontend/src/App.svelte` and `frontend/src/pages/PhoneNumberPage.svelte`.
- [x] T405 [Plan:4.4,5.5,6.3] Extend `frontend/src/js/api.js`, `frontend/src/js/i18n.js`, and `frontend/src/pages/SimSetPhonePage.svelte` for local Multi-SIM number editing, per-slot states, and recoverable operation progress; align the shared phone handler in `src/modem/manager.rs` with this database-only rule.
- [x] T406 [Plan:6.4,7] Integrate verified selection and switching states into `frontend/src/pages/SimDashboard.svelte`, without changing normal-machine or eSIM behavior.

### Phase 5: Hardware testing

- Machine type detection on all three hardware types.
- eSIM: download two or more profiles, switch enabled profile, delete disabled profile, recover with `ath0`.
- Multi-SIM: switch slots 1–4, empty slot handling, restart while switched, SMS receive after switch.

Current user constraint: manual network signal/registration checks are limited to **COM1 slots 1 and 2**. A four-slot identity scan is not permission to extend RF testing to other slots or ports. The RX/TX-only hardware must not acquire a DTR-based requirement. The eSIM deletion/reset scenarios above are future test ideas, not authorization to execute them.

## 9. Multi-SIM Release Decisions

Initial-release decisions:

- Explicit first scan rather than automatic card rotation when opening the page.
- Restore the original verified slot after discovery; ask for a final slot if the original selection is unknown.
- Database-only number edits; ICCID follows the card, while port/slot records only its observed location.
- Initial release focuses on one port's four slots; reset, automatic rotation, and multi-port batch actions are not part of this release.

## 10. Hardware Test Plan

| Machine | Status | Tests |
|---|---|---|
| eSIM | Connected now | `ath?` confirmed; next: multi-profile download, enable/disable, delete, 20-profile limit, exit and SMS receive |
| Multi-SIM | Connected | `ath?` confirmed `MSIM`; `ath1`–`ath4` confirmed; next: `NO SIM`, reboot to slot 1, SMS receive/send on a registered SIM |
| Normal | Later | `ath?` returns `ERROR`; dashboard unchanged |

## 11. Multi-SIM Requirement Mapping

This is the approved implementation's scope checklist. Checked tasks describe source implementation, not blanket hardware certification.

| Requirement | Proposed behavior | Plan sections | Tasks |
|---|---|---|---|
| REQ-MSIM-01 | Four visible physical slots in a drawer based on the eSIM manager | 6.3 | T404 |
| REQ-MSIM-02 | Sequential ICCID/IMSI discovery and per-slot database persistence | 4.2, 5.5 | T401, T402, T403 |
| REQ-MSIM-03 | Best-effort MSISDN discovery and ICCID-linked manual local numbers | 4.4, 5.5, 6.3 | T401, T403, T405 |
| REQ-MSIM-04 | Deliberate slot selection with stable identity readback and accurate dashboard state | 2.3, 5.5, 6.4 | T402, T403, T406 |
| REQ-MSIM-05 | Preserve the original selection after scans and expose restoration failures | 5.5, 6.3 | T402, T403, T405 |
| REQ-MSIM-06 | Prevent worker conflicts and sends under the wrong ICCID | 5.5, 7 | T402, T406 |
| REQ-MSIM-07 | Recover progress after dropped streams, page reloads, or interrupted jobs | 5.5, 6.3 | T401, T403, T405 |
| REQ-MSIM-08 | Distinguish stale, unread, empty, missing-number, and failed states on desktop/mobile | 4.2, 4.4, 6.3 | T401, T404, T405 |

## 12. Implementation Verification (2026-10-31)

- Rust: 67 passed, 0 failed, 3 opt-in tests ignored. After the Windows error-mapping correction, all 16 focused Multi-SIM tests passed again.
- Frontend production build passed. Populated drawer screenshots checked at 1480x900 and 390x844; all four rows and their controls fit without horizontal drawer overflow.
- Drawer focus-trap and Escape/return-focus handlers passed synthetic DOM keyboard checks. Native Playwright keyboard delivery in the embedded browser was inconclusive.
- Silent SSE fixture recovered by status polling in 2425 ms; controls unlocked. All test routes were removed afterward.
- Authentication and invalid port/slot/ICCID/number requests were checked without hardware writes. Persisted operation SSE returns an initial snapshot, including after completion.
- Real COM1 identity scan `1383a79b-7ca5-41fb-bc7c-7bed0466921c` read all four slots and restored slot 1 with stable ICCID verification. No manual RF probes were sent to slots 3/4 or COM2.
- All 18 saved numbers matched the stopped-process backup. The drawer saved slot 1's unchanged number through the database-only route; readback confirmed manual provenance.
- Windows handoff initially failed before any slot command. Awaited task shutdown and worker quarantine prevent competing owners, but the serial driver can release its handle asynchronously. `serialport` 4.7.0 maps Windows access-denied to `NoDevice`, which this gateway maps to `io::ErrorKind::Other`. Exclusive MSIM open now retries that generic error and permission-denied at 100 ms intervals for at most 3 seconds. Other typed errors fail immediately; permanently busy ports still fail and stay paused.
- Final corrected-build handoff passed from a normally running COM1. Selection `5f409f45-4674-4707-8355-c5a0ec79a24c` completed on slot 2 with matching ICCID; duplicate request IDs returned the same operation and a competing scan returned `409`.
- Cancellation `af166e3f-37d5-437f-8097-adf1ea7d5907` passed through the real UI. Closing/reopening the drawer left the operation running. Explicit Cancel finished the in-flight slot-1 read, skipped slots 2-4, restored and verified slot 1, and unlocked controls. Live SSE showed restoring then cancelled.
- Dashboard readback showed COM1 / MSIM / slot 1, ICCID 89441000304695563235, and number 7435449832. A full page reload preserved this state; test globals were cleared.
- Final portable release is running at `http://127.0.0.1:8080`, PID 10924. SHA256: `9C5C74DB6F9D1DF16A076DF141A6507B4535FFDEF030DDABEE79A7FBC078B409`; served frontend: `/assets/index-BiY2C9FS.js`. Configuration and database files were not replaced.
- Empty-slot, PIN-locked-card, physical unplug during a scan, interrupted-process recovery, and post-switch SMS delivery are not hardware-certified by these checks. No eSIM profile mutation, QR resubmission, DTR change, commit, or push was performed.

Observed COM1 inventory from the successful identity scan:

| Slot | ICCID | Saved number |
|---|---|---|
| 1 | 89441000304695563235 | 7435449832 |
| 2 | 89441000305145631910 | 7341629240 |
| 3 | 89441000305143634965 | 7823608733 |
| 4 | 89441000305132003313 | 7979079625 |
