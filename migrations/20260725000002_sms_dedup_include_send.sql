-- Allow self-sent loopback receive rows to coexist with outbound rows.
-- Previous dedup key did not include direction, so incoming self-SMS could be
-- ignored when it matched contact/sim/timestamp/message of the outbound send.
DROP INDEX IF EXISTS idx_sms_dedup;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sms_dedup
    ON sms (contact_id, sim_id, timestamp, message, send);
