CREATE TABLE process_names (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    process_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    current_revision INT UNSIGNED NOT NULL DEFAULT 0,
    UNIQUE KEY process_name (workspace_id, process_key),
    UNIQUE KEY scoped_process (workspace_id, id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_definitions (
    workspace_id BIGINT UNSIGNED NOT NULL,
    process_id BIGINT UNSIGNED NOT NULL,
    revision INT UNSIGNED NOT NULL,
    definition JSON NOT NULL,
    digest BINARY(32) NOT NULL,
    author VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    published_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (workspace_id, process_id, revision),
    FOREIGN KEY (workspace_id, process_id) REFERENCES process_names(workspace_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_runs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    process_id BIGINT UNSIGNED NOT NULL,
    start_revision INT UNSIGNED NOT NULL,
    definition_rev INT UNSIGNED NOT NULL,
    business_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    input JSON NOT NULL,
    input_digest BINARY(32) NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status ENUM('running','waiting','succeeded','failed','cancelling','reconcile','cancelled') NOT NULL DEFAULT 'running',
    step_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    transition_rev BIGINT UNSIGNED NOT NULL DEFAULT 1,
    started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY scoped_process_run (workspace_id, id),
    UNIQUE KEY business_run (workspace_id, process_id, business_key),
    KEY process_run_status (workspace_id, status, id),
    FOREIGN KEY (workspace_id, process_id, definition_rev) REFERENCES process_definitions(workspace_id, process_id, revision)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_waits (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    definition_rev INT UNSIGNED NOT NULL,
    step_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    iteration BIGINT UNSIGNED NOT NULL,
    kind ENUM('signal','approval','timer') NOT NULL,
    signal_kind VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    deadline DATETIME(6) NOT NULL,
    status ENUM('waiting','accepted','rejected','timeout','cancelled','migrated') NOT NULL DEFAULT 'waiting',
    winner_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    resolved_at DATETIME(6) NULL,
    timer_observed_at DATETIME(6) NULL,
    active_run BIGINT UNSIGNED GENERATED ALWAYS AS (IF(status='waiting',run_id,NULL)) STORED,
    UNIQUE KEY one_pending_wait (workspace_id, active_run),
    UNIQUE KEY wait_iteration (workspace_id, run_id, step_key, iteration),
    KEY due_process_wait (status, deadline, id),
    KEY run_wait_history (workspace_id, run_id, id),
    FOREIGN KEY (workspace_id, run_id) REFERENCES process_runs(workspace_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_signals (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    event_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    correlation_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    kind VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload JSON NOT NULL,
    payload_hash BINARY(32) NOT NULL,
    outcome ENUM('waiting_for_match','consumed','late','expired') NOT NULL DEFAULT 'waiting_for_match',
    wait_id BIGINT UNSIGNED NULL,
    received_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    matched_at DATETIME(6) NULL,
    consumed_at DATETIME(6) NULL,
    expires_at DATETIME(6) NOT NULL,
    UNIQUE KEY signal_event_key (workspace_id, run_id, event_key),
    UNIQUE KEY scoped_process_signal (workspace_id, run_id, id),
    KEY signal_match (workspace_id, run_id, kind, outcome, id),
    KEY signal_expiration (outcome, expires_at, id),
    FOREIGN KEY (workspace_id, run_id) REFERENCES process_runs(workspace_id, id),
    FOREIGN KEY (wait_id) REFERENCES process_waits(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_signal_requests (
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    signal_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (workspace_id, run_id, request_key),
    FOREIGN KEY (workspace_id, run_id, signal_id) REFERENCES process_signals(workspace_id, run_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    definition_rev INT UNSIGNED NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    kind VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    details JSON NOT NULL,
    occurred_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY process_history (workspace_id, run_id, id),
    FOREIGN KEY (workspace_id, run_id) REFERENCES process_runs(workspace_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
