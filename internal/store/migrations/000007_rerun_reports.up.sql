ALTER TABLE runs ADD COLUMN operation_trigger_key VARBINARY(128);

ALTER TABLE node_runs
    ADD COLUMN reused_from_run_id BIGINT UNSIGNED,
    ADD FOREIGN KEY (reused_from_run_id, node_key) REFERENCES node_runs(run_id, node_key);

ALTER TABLE attempts ADD COLUMN log_excerpt TEXT, ADD COLUMN log_digest BINARY(32);

CREATE TABLE reruns (
    new_run_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    parent_run_id BIGINT UNSIGNED NOT NULL,
    request_key VARBINARY(128) NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    plan JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY rerun_request (parent_run_id, request_key),
    KEY rerun_parent (parent_run_id, new_run_id),
    FOREIGN KEY (workspace_id, dag_id, parent_run_id) REFERENCES runs(workspace_id, dag_id, id),
    FOREIGN KEY (workspace_id, dag_id, new_run_id) REFERENCES runs(workspace_id, dag_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
