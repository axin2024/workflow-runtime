CREATE TABLE process_intake_controls (
    workspace_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    paused BOOLEAN NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE process_actions ADD KEY pending_run_actions (workspace_id,run_id,status);

ALTER TABLE process_waits
    ADD COLUMN watch_deadline DATETIME(6) GENERATED ALWAYS AS (IF(status IN ('waiting','accepted','rejected') AND timer_observed_at IS NULL,deadline,NULL)) STORED,
    ADD KEY pending_process_timer (watch_deadline,id);
