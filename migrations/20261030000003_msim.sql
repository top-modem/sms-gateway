CREATE TABLE msim_slots (
    com_port TEXT NOT NULL,
    slot INTEGER NOT NULL CHECK (slot BETWEEN 1 AND 4),
    state TEXT NOT NULL DEFAULT 'unknown',
    iccid TEXT,
    imsi TEXT,
    last_checked_at TEXT,
    identity_verified_at TEXT,
    last_error TEXT,
    PRIMARY KEY (com_port, slot)
);

CREATE INDEX idx_msim_slots_iccid ON msim_slots(iccid);

ALTER TABLE sim_cards ADD COLUMN phone_number_source TEXT;

CREATE TABLE msim_operations (
    id TEXT PRIMARY KEY,
    com_port TEXT NOT NULL,
    request_id TEXT,
    status TEXT NOT NULL,
    snapshot TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_msim_running_port ON msim_operations(com_port)
WHERE status = 'running';

CREATE UNIQUE INDEX idx_msim_request ON msim_operations(com_port, request_id)
WHERE request_id IS NOT NULL;

CREATE INDEX idx_msim_operations_port ON msim_operations(com_port, updated_at);