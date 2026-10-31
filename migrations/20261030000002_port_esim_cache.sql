UPDATE port_hardware
SET eid = NULL,
    profiles_refreshed_at = NULL,
    updated_at = datetime('now')
WHERE machine_type IN ('normal', 'msim');

DELETE FROM esim_profiles
WHERE com_port IN (
    SELECT com_port FROM port_hardware
    WHERE machine_type IN ('normal', 'msim')
);