ALTER TABLE runs ADD UNIQUE KEY schedule_occurrence (schedule_id, logical_at);

CREATE TABLE backfill_batches (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    schedule_id BIGINT UNSIGNED NOT NULL,
    schedule_generation BIGINT UNSIGNED NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    timezone VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    from_at DATETIME(6) NOT NULL,
    through_at DATETIME(6) NOT NULL,
    input_values MEDIUMTEXT NOT NULL,
    input_digest BINARY(32) NOT NULL,
    selection_digest BINARY(32),
    state ENUM('draft', 'confirmed', 'completed') NOT NULL DEFAULT 'draft',
    created_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    confirmed_at DATETIME(6),
    completed_at DATETIME(6),
    UNIQUE KEY scoped_batch (workspace_id, dag_id, schedule_id, id),
    KEY workspace_batch (workspace_id, id),
    FOREIGN KEY (workspace_id, dag_id, schedule_id) REFERENCES schedules(workspace_id, dag_id, id),
    FOREIGN KEY (dag_id, revision) REFERENCES dag_versions(dag_id, revision),
    CHECK (JSON_VALID(input_values)),
    CHECK (from_at <= through_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE backfill_items (
    batch_id BIGINT UNSIGNED NOT NULL,
    ordinal INT UNSIGNED NOT NULL,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    schedule_id BIGINT UNSIGNED NOT NULL,
    logical_at DATETIME(6) NOT NULL,
    local_at VARCHAR(48) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    state ENUM('available', 'already_registered', 'excluded', 'pending', 'created', 'reused') NOT NULL,
    baseline_run_id BIGINT UNSIGNED,
    result_run_id BIGINT UNSIGNED,
    processed_at DATETIME(6),
    reserved_at DATETIME(6) GENERATED ALWAYS AS (IF(state = 'pending', logical_at, NULL)) VIRTUAL,
    PRIMARY KEY (batch_id, ordinal),
    UNIQUE KEY batch_occurrence (batch_id, logical_at),
    UNIQUE KEY schedule_reservation (schedule_id, reserved_at),
    KEY batch_pending (batch_id, state, ordinal),
    FOREIGN KEY (workspace_id, dag_id, schedule_id, batch_id) REFERENCES backfill_batches(workspace_id, dag_id, schedule_id, id),
    FOREIGN KEY (workspace_id, dag_id, baseline_run_id) REFERENCES runs(workspace_id, dag_id, id),
    FOREIGN KEY (workspace_id, dag_id, result_run_id) REFERENCES runs(workspace_id, dag_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
