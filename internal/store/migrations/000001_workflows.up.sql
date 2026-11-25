CREATE TABLE workspaces (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY workspace_key (workspace_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE dags (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    active_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
    UNIQUE KEY workspace_dag_key (workspace_id, dag_key),
    UNIQUE KEY workspace_dag_id (workspace_id, id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE dag_versions (
    dag_id BIGINT UNSIGNED NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    digest BINARY(32) NOT NULL,
    definition MEDIUMTEXT NOT NULL,
    publisher VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    published_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (dag_id, revision),
    FOREIGN KEY (dag_id) REFERENCES dags(id),
    CHECK (JSON_VALID(definition))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE dag_edges (
    dag_id BIGINT UNSIGNED NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    from_node VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    to_node VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    edge_condition ENUM('succeeded', 'finished', 'failed', 'branch') NOT NULL,
    branch VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (dag_id, revision, from_node, to_node),
    FOREIGN KEY (dag_id, revision) REFERENCES dag_versions(dag_id, revision)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE runs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    revision BIGINT UNSIGNED NOT NULL,
    trigger_kind VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    trigger_key VARBINARY(128) NOT NULL,
    request_digest BINARY(32) NOT NULL,
    input_values MEDIUMTEXT NOT NULL,
    logical_at DATETIME(6) NOT NULL,
    state VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY dag_trigger (workspace_id, dag_id, trigger_key),
    UNIQUE KEY dag_run (workspace_id, dag_id, id),
    KEY revision_runs (dag_id, revision),
    FOREIGN KEY (workspace_id, dag_id) REFERENCES dags(workspace_id, id),
    FOREIGN KEY (dag_id, revision) REFERENCES dag_versions(dag_id, revision),
    CHECK (JSON_VALID(input_values))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE node_runs (
    run_id BIGINT UNSIGNED NOT NULL,
    node_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    task VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    queue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    state VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    available_at DATETIME(6),
    PRIMARY KEY (run_id, node_key),
    KEY queue_ready (queue, state, available_at, run_id, node_key),
    FOREIGN KEY (run_id) REFERENCES runs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE audit_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_kind VARCHAR(48) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    details JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY workspace_log (workspace_id, id),
    KEY dag_run_log (workspace_id, dag_id, run_id, id),
    FOREIGN KEY (workspace_id, dag_id) REFERENCES dags(workspace_id, id),
    FOREIGN KEY (workspace_id, dag_id, run_id) REFERENCES runs(workspace_id, dag_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
