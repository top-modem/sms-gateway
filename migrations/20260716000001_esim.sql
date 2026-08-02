-- eSIM support: audit log of eSIM management operations and a cache of
-- downloaded/known eSIM profiles per COM port. See esim_feature_design.txt.

CREATE TABLE IF NOT EXISTS esim_operations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    com_port     TEXT    NOT NULL,
    eid          TEXT,
    op_type      TEXT    NOT NULL, -- detect/enter/exit/reset/download/enable/disable/delete/nickname/notification/provision
    params_json  TEXT,
    result_code  INTEGER,          -- lpac payload.code (0 = success) or NULL for pure AT ops
    message      TEXT,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_esim_operations_com_port ON esim_operations (com_port);
CREATE INDEX IF NOT EXISTS idx_esim_operations_created_at ON esim_operations (created_at);

CREATE TABLE IF NOT EXISTS esim_profiles (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    com_port              TEXT NOT NULL,
    eid                   TEXT,
    iccid                 TEXT NOT NULL,
    isdp_aid              TEXT,
    profile_state         TEXT,  -- enabled / disabled
    nickname              TEXT,
    service_provider_name TEXT,
    profile_name          TEXT,
    profile_class         TEXT,
    updated_at            TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (com_port, iccid)
);

CREATE INDEX IF NOT EXISTS idx_esim_profiles_com_port ON esim_profiles (com_port);
