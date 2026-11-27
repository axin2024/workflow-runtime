CREATE TABLE process_duty_roles (
    identity_id BIGINT UNSIGNED NOT NULL,
    duty_role VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (identity_id, duty_role),
    FOREIGN KEY (identity_id) REFERENCES console_identities(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_tasks (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    wait_id BIGINT UNSIGNED NOT NULL,
    duty_role VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status ENUM('pending','claimed','approved','rejected','expired','migrated','cancelled') NOT NULL DEFAULT 'pending',
    claim_rev BIGINT UNSIGNED NOT NULL DEFAULT 1,
    assigned_actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    claim_until DATETIME(6) NULL,
    completed_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    completed_at DATETIME(6) NULL,
    UNIQUE KEY task_wait (wait_id),
    UNIQUE KEY scoped_process_task (workspace_id, id),
    KEY process_task_queue (workspace_id, duty_role, status, id),
    KEY process_task_history (workspace_id, run_id, id),
    FOREIGN KEY (workspace_id, run_id) REFERENCES process_runs(workspace_id, id),
    FOREIGN KEY (wait_id) REFERENCES process_waits(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_task_requests (
    workspace_id BIGINT UNSIGNED NOT NULL,
    task_id BIGINT UNSIGNED NOT NULL,
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    digest BINARY(32) NOT NULL,
    receipt JSON NOT NULL,
    conflict BOOLEAN NOT NULL,
    PRIMARY KEY (workspace_id, task_id, request_key),
    FOREIGN KEY (workspace_id, task_id) REFERENCES process_tasks(workspace_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
