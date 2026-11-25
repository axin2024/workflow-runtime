ALTER TABLE attempts ADD COLUMN log_cleared_at DATETIME(6);

CREATE TABLE retention_log_queue (
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    node_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ordinal BIGINT UNSIGNED NOT NULL,
    finished_at DATETIME(6) NOT NULL,
    PRIMARY KEY (run_id, node_key, ordinal),
    KEY retention_candidates (workspace_id, finished_at, run_id, node_key, ordinal),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
    FOREIGN KEY (run_id, node_key, ordinal) REFERENCES attempts(run_id, node_key, ordinal)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

INSERT INTO retention_log_queue (workspace_id, run_id, node_key, ordinal, finished_at)
SELECT r.workspace_id, a.run_id, a.node_key, a.ordinal, a.finished_at
FROM attempts a JOIN runs r ON r.id = a.run_id
WHERE a.log_excerpt IS NOT NULL AND a.finished_at IS NOT NULL;

CREATE TABLE log_retention_policies (
    workspace_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    generation BIGINT UNSIGNED NOT NULL,
    days SMALLINT UNSIGNED NOT NULL CHECK (days <= 3650),
    updated_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE log_retention_jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    request_key VARBINARY(128) NOT NULL,
    generation BIGINT UNSIGNED NOT NULL,
    cutoff DATETIME(6) NOT NULL,
    committed_at DATETIME(6) NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    batch_limit SMALLINT UNSIGNED NOT NULL,
    has_more BOOLEAN NOT NULL,
    cleared JSON NOT NULL,
    UNIQUE KEY retention_request (workspace_id, request_key),
    KEY retention_history (workspace_id, id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
