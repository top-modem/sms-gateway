-- Per-port hardware identity: machine type from `ath?`, eUICC EID, and the
-- stored Multi-SIM slot (the hardware cannot report its current slot).
CREATE TABLE IF NOT EXISTS port_hardware (
    com_port              TEXT PRIMARY KEY,
    machine_type          TEXT NOT NULL DEFAULT 'unknown', -- normal / esim / msim / unknown
    type_reply            TEXT,
    eid                   TEXT,
    active_slot           INTEGER,
    profiles_refreshed_at TEXT,
    detected_at           TEXT,
    updated_at            TEXT NOT NULL DEFAULT (datetime('now'))
);
