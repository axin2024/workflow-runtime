CREATE TABLE process_child_groups (
    workspace_id BIGINT UNSIGNED NOT NULL,
    parent_run_id BIGINT UNSIGNED NOT NULL,
    iteration BIGINT UNSIGNED NOT NULL,
    definition_rev INT UNSIGNED NOT NULL,
    step_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status ENUM('starting','waiting','succeeded','failed','cancelled') NOT NULL DEFAULT 'starting',
    required_successes SMALLINT UNSIGNED NOT NULL,
    finished_at DATETIME(6) NULL,
    PRIMARY KEY (workspace_id, parent_run_id, iteration),
    KEY pending_child_groups (status, parent_run_id),
    FOREIGN KEY (workspace_id, parent_run_id) REFERENCES process_runs(workspace_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE process_child_links (
    workspace_id BIGINT UNSIGNED NOT NULL,
    parent_run_id BIGINT UNSIGNED NOT NULL,
    iteration BIGINT UNSIGNED NOT NULL,
    ordinal SMALLINT UNSIGNED NOT NULL,
    child_run_id BIGINT UNSIGNED NOT NULL,
    pinned_revision INT UNSIGNED NOT NULL,
    observed_status ENUM('succeeded','failed','cancelled') NULL,
    cancel_actor VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    cancel_requested_at DATETIME(6) NULL,
    cancel_delivered_at DATETIME(6) NULL,
    pending_cancel BIGINT UNSIGNED GENERATED ALWAYS AS (IF(cancel_requested_at IS NOT NULL AND cancel_delivered_at IS NULL, child_run_id, NULL)) STORED,
    PRIMARY KEY (workspace_id, parent_run_id, iteration, ordinal),
    UNIQUE KEY child_membership (workspace_id, child_run_id),
    KEY pending_child_cancellations (pending_cancel),
    FOREIGN KEY (workspace_id, parent_run_id, iteration) REFERENCES process_child_groups(workspace_id, parent_run_id, iteration),
    FOREIGN KEY (workspace_id, child_run_id) REFERENCES process_runs(workspace_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE process_runs ADD KEY terminal_children (status, workspace_id, id);
