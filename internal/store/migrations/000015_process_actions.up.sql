CREATE TABLE process_actions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    step_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    iteration BIGINT UNSIGNED NOT NULL,
    direction ENUM('forward','reverse') NOT NULL,
    original_id BIGINT UNSIGNED NULL,
    operation_name VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    reverse_operation VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    operation_key VARCHAR(80) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status ENUM('ready','leased','succeeded','failed','unknown','discarded') NOT NULL DEFAULT 'ready',
    lease_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
    leased_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    lease_until DATETIME(6) NULL,
    external_receipt VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    evidence JSON NULL,
    resolution_owner VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    resolution VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    confirmed_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY scoped_process_action (workspace_id, id),
    UNIQUE KEY process_action_step (workspace_id,run_id,iteration,direction),
    UNIQUE KEY one_reverse_action (original_id),
    UNIQUE KEY operation_identity (workspace_id,operation_key),
    KEY process_action_queue (workspace_id,operation_name,status,id),
    KEY expired_process_action (status,lease_until,id),
    FOREIGN KEY (workspace_id,run_id) REFERENCES process_runs(workspace_id,id),
    FOREIGN KEY (workspace_id,original_id) REFERENCES process_actions(workspace_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_action_requests (
    workspace_id BIGINT UNSIGNED NOT NULL,
    action_id BIGINT UNSIGNED NOT NULL,
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    digest BINARY(32) NOT NULL,
    receipt JSON NOT NULL,
    PRIMARY KEY (workspace_id,action_id,request_key),
    FOREIGN KEY (workspace_id,action_id) REFERENCES process_actions(workspace_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_cancellations (
    workspace_id BIGINT UNSIGNED NOT NULL,
    run_id BIGINT UNSIGNED NOT NULL,
    initiator VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    initial_status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status ENUM('waiting','reconcile','succeeded','stopped') NOT NULL DEFAULT 'waiting',
    next_scan_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    finished_at DATETIME(6) NULL,
    PRIMARY KEY (workspace_id,run_id),
    KEY due_process_cancellation (status,next_scan_at,run_id),
    FOREIGN KEY (workspace_id,run_id) REFERENCES process_runs(workspace_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE process_child_groups ADD COLUMN reversal_status ENUM('none','waiting','succeeded','stopped') NOT NULL DEFAULT 'none';
ALTER TABLE process_child_links
    ADD COLUMN reverse_actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD COLUMN reverse_requested_at DATETIME(6) NULL,
    ADD COLUMN reverse_delivered_at DATETIME(6) NULL,
    ADD COLUMN pending_reverse BIGINT UNSIGNED GENERATED ALWAYS AS (IF(reverse_requested_at IS NOT NULL AND reverse_delivered_at IS NULL, child_run_id, NULL)) STORED,
    ADD KEY pending_child_reversals (pending_reverse);
