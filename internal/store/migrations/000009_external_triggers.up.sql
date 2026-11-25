CREATE TABLE trigger_identities (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token_digest BINARY(32),
    UNIQUE KEY trigger_actor (workspace_id, dag_id, actor),
    UNIQUE KEY trigger_identity_scope (workspace_id, dag_id, id),
    UNIQUE KEY trigger_token (token_digest),
    FOREIGN KEY (workspace_id, dag_id) REFERENCES dags(workspace_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE trigger_receipts (
    run_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    dag_id BIGINT UNSIGNED NOT NULL,
    identity_id BIGINT UNSIGNED NOT NULL,
    FOREIGN KEY (workspace_id, dag_id, run_id) REFERENCES runs(workspace_id, dag_id, id),
    FOREIGN KEY (workspace_id, dag_id, identity_id) REFERENCES trigger_identities(workspace_id, dag_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
