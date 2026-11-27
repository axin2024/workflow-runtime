CREATE TABLE process_migration_batches (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    process_id BIGINT UNSIGNED NOT NULL,
    source_rev INT UNSIGNED NOT NULL,
    target_rev INT UNSIGNED NOT NULL,
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    digest BINARY(32) NOT NULL,
    mapping JSON NOT NULL,
    previewed_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    applied_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    previewed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    applied_at DATETIME(6) NULL,
    UNIQUE KEY scoped_process_migration (workspace_id,id),
    UNIQUE KEY process_migration_request (workspace_id,request_key),
    FOREIGN KEY (workspace_id,process_id,source_rev) REFERENCES process_definitions(workspace_id,process_id,revision),
    FOREIGN KEY (workspace_id,process_id,target_rev) REFERENCES process_definitions(workspace_id,process_id,revision)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_migration_targets (
    workspace_id BIGINT UNSIGNED NOT NULL,
    migration_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    observed_wait_id BIGINT UNSIGNED NULL,
    new_wait_id BIGINT UNSIGNED NULL,
    observed_transition BIGINT UNSIGNED NULL,
    source_step VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    target_step VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    status ENUM('ready','skipped','migrated') NOT NULL,
    reason VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    PRIMARY KEY (workspace_id,migration_id,run_id),
    FOREIGN KEY (workspace_id,migration_id) REFERENCES process_migration_batches(workspace_id,id),
    FOREIGN KEY (observed_wait_id) REFERENCES process_waits(id),
    FOREIGN KEY (new_wait_id) REFERENCES process_waits(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
