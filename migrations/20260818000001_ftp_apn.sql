-- Separate APN for FTP host-staged MMS attachment downloads.
ALTER TABLE sim_cards ADD COLUMN ftp_apn TEXT;
