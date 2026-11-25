CREATE TABLE schedules (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    schedule_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    generation BIGINT UNSIGNED NOT NULL,
    cron_expression VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    timezone VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    misfire ENUM('catch_up', 'skip') NOT NULL,
    grace_seconds INT UNSIGNED NOT NULL,
    max_active_runs INT UNSIGNED NOT NULL,
    pinned_revision BIGINT UNSIGNED,
    input_values MEDIUMTEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    start_after DATETIME(6) NOT NULL,
    next_at DATETIME(6),
    scan_after DATETIME(6),
    last_error VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
    UNIQUE KEY dag_schedule_key (workspace_id, dag_id, schedule_key),
    UNIQUE KEY dag_schedule_id (workspace_id, dag_id, id),
    KEY due_schedules (enabled, scan_after, id, workspace_id, dag_id),
    FOREIGN KEY (workspace_id, dag_id) REFERENCES dags(workspace_id, id),
    FOREIGN KEY (dag_id, pinned_revision) REFERENCES dag_versions(dag_id, revision),
    CHECK (JSON_VALID(input_values)),
    CHECK (max_active_runs BETWEEN 1 AND 10000),
    CHECK (grace_seconds <= 86400)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE runs
    ADD COLUMN schedule_id BIGINT UNSIGNED,
    ADD KEY schedule_active (schedule_id, state),
    ADD FOREIGN KEY (workspace_id, dag_id, schedule_id) REFERENCES schedules(workspace_id, dag_id, id);
