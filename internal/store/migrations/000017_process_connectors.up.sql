CREATE TABLE process_connector_grants (
    workspace_id BIGINT UNSIGNED NOT NULL,
    process_id BIGINT UNSIGNED NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    operation_name VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token_digest BINARY(32) NOT NULL,
    granted_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (workspace_id,process_id,actor,operation_name),
    UNIQUE KEY connector_digest (token_digest),
    FOREIGN KEY (workspace_id,process_id) REFERENCES process_names(workspace_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
