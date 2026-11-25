CREATE TABLE workers (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    worker_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token_digest BINARY(32) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_active INT UNSIGNED NOT NULL,
    created_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY workspace_worker (workspace_id, worker_key),
    UNIQUE KEY worker_token (token_digest),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
    CHECK (max_active BETWEEN 1 AND 64)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE worker_permissions (
    worker_id BIGINT UNSIGNED NOT NULL,
    queue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    task VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (worker_id, queue, task),
    FOREIGN KEY (worker_id) REFERENCES workers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE runs ADD COLUMN finished_at DATETIME(6);

ALTER TABLE node_runs
    ADD COLUMN lease_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN lease_worker_id BIGINT UNSIGNED,
    ADD COLUMN lease_until DATETIME(6),
    ADD COLUMN deadline_at DATETIME(6),
    ADD COLUMN finished_at DATETIME(6),
    ADD COLUMN result_branch VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
    ADD COLUMN output_values MEDIUMTEXT,
    ADD COLUMN output_digest BINARY(32),
    ADD COLUMN failure_code VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
    ADD KEY worker_live_leases (lease_worker_id, lease_until),
    ADD KEY expired_leases (queue, state, lease_until, run_id, node_key),
    ADD FOREIGN KEY (lease_worker_id) REFERENCES workers(id),
    ADD CHECK (output_values IS NULL OR JSON_VALID(output_values));

CREATE TABLE attempts (
    run_id BIGINT UNSIGNED NOT NULL,
    node_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ordinal BIGINT UNSIGNED NOT NULL,
    worker_id BIGINT UNSIGNED NOT NULL,
    operation_key BINARY(32) NOT NULL,
    state VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    node_state VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin,
    queued_at DATETIME(6) NOT NULL,
    started_at DATETIME(6) NOT NULL,
    lease_until DATETIME(6) NOT NULL,
    deadline_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6),
    result_digest BINARY(32),
    failure_code VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
    failure_message VARCHAR(1024),
    PRIMARY KEY (run_id, node_key, ordinal),
    FOREIGN KEY (run_id, node_key) REFERENCES node_runs(run_id, node_key),
    FOREIGN KEY (worker_id) REFERENCES workers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE edge_results (
    run_id BIGINT UNSIGNED NOT NULL,
    to_node VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    from_node VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    satisfied BOOLEAN NOT NULL,
    reason VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    decided_at DATETIME(6) NOT NULL,
    PRIMARY KEY (run_id, to_node, from_node),
    FOREIGN KEY (run_id, to_node) REFERENCES node_runs(run_id, node_key),
    FOREIGN KEY (run_id, from_node) REFERENCES node_runs(run_id, node_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
