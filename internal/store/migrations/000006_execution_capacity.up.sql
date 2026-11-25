CREATE TABLE execution_pools (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    pool_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
    max_active INT UNSIGNED NOT NULL,
    generation BIGINT UNSIGNED NOT NULL,
    updated_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    CHECK (max_active <= 10000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE workspace_capacity (
    workspace_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    max_active INT UNSIGNED,
    generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
    updated_at DATETIME(6),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
    CHECK (max_active <= 10000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

INSERT INTO workspace_capacity (workspace_id) SELECT id FROM workspaces;

CREATE TABLE queue_capacity (
    workspace_id BIGINT UNSIGNED NOT NULL,
    queue_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    pool_id BIGINT UNSIGNED,
    max_active INT UNSIGNED,
    generation BIGINT UNSIGNED NOT NULL,
    updated_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (workspace_id, queue_key),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
    FOREIGN KEY (pool_id) REFERENCES execution_pools(id),
    CHECK (max_active <= 10000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE node_runs
    ADD COLUMN lease_pool_id BIGINT UNSIGNED,
    ADD KEY pool_live_leases (lease_pool_id, lease_until),
    ADD FOREIGN KEY (lease_pool_id) REFERENCES execution_pools(id);

ALTER TABLE attempts
    ADD COLUMN pool_id BIGINT UNSIGNED,
    ADD FOREIGN KEY (pool_id) REFERENCES execution_pools(id);
