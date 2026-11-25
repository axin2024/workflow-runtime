CREATE TABLE console_identities (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    role ENUM('author', 'operator', 'viewer') NOT NULL,
    token_digest BINARY(32) NOT NULL,
    UNIQUE KEY console_actor (workspace_id, actor),
    UNIQUE KEY console_token (token_digest),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE console_sessions (
    token_digest BINARY(32) NOT NULL PRIMARY KEY,
    identity_id BIGINT UNSIGNED NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    KEY console_expiration (expires_at),
    FOREIGN KEY (identity_id) REFERENCES console_identities(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE runs ADD KEY workspace_recent_runs (workspace_id, id);

ALTER TABLE audit_events ADD KEY workspace_run_audit (workspace_id, run_id, id);
