CREATE TABLE cancel_requests (
    run_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    request_key VARBINARY(128) NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    reason VARCHAR(1024) NOT NULL,
    grace_seconds SMALLINT UNSIGNED NOT NULL,
    requested_at DATETIME(6) NOT NULL,
    deadline_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6),
    FOREIGN KEY (run_id) REFERENCES runs(id),
    CHECK (grace_seconds <= 120)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE external_resolutions (
    run_id BIGINT UNSIGNED NOT NULL,
    node_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ordinal BIGINT UNSIGNED NOT NULL,
    request_key VARBINARY(128) NOT NULL,
    request_digest BINARY(32) NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    evidence VARCHAR(1024) NOT NULL,
    state ENUM('succeeded', 'failed') NOT NULL,
    resolved_at DATETIME(6) NOT NULL,
    PRIMARY KEY (run_id, node_key),
    FOREIGN KEY (run_id, node_key, ordinal) REFERENCES attempts(run_id, node_key, ordinal)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE node_runs ADD KEY execution_deadline (state, lease_until, run_id, node_key);
