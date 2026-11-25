ALTER TABLE trigger_receipts ADD UNIQUE KEY trigger_receipt_owner (identity_id, run_id);

CREATE TABLE callback_deliveries (
    run_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    identity_id BIGINT UNSIGNED NOT NULL,
    event_id BINARY(16) NOT NULL,
    attempts BIGINT UNSIGNED NOT NULL DEFAULT 0,
    available_at DATETIME(6) NOT NULL,
    lease_token BINARY(32),
    last_http_status SMALLINT UNSIGNED,
    delivered_at DATETIME(6),
    UNIQUE KEY callback_event (event_id),
    KEY callback_ready (identity_id, delivered_at, available_at, run_id),
    FOREIGN KEY (identity_id, run_id) REFERENCES trigger_receipts(identity_id, run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

INSERT INTO callback_deliveries (run_id, identity_id, event_id, available_at)
SELECT t.run_id, t.identity_id, RANDOM_BYTES(16), r.finished_at
FROM trigger_receipts t JOIN runs r ON r.id = t.run_id
WHERE r.state IN ('succeeded', 'failed', 'canceled') AND r.finished_at IS NOT NULL;
