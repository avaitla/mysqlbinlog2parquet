-- Exercise a wider range of column types.
CREATE TABLE IF NOT EXISTS metrics (
    id BIGINT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    value DOUBLE,
    flag BOOLEAN NOT NULL,
    payload JSON,
    captured_at DATETIME(3) NOT NULL
);

INSERT INTO metrics (id, name, value, flag, payload, captured_at) VALUES
    (1, 'metric_a', 1.5,  TRUE,  JSON_OBJECT('k', 1), '2024-01-01 00:00:00.000'),
    (2, 'metric_b', 2.25, FALSE, JSON_ARRAY(1, 2, 3),  '2024-01-01 00:00:01.500'),
    (3, 'metric_c', NULL, TRUE,  NULL,                 '2024-01-01 00:00:02.250');

UPDATE metrics SET value = 9.99 WHERE id = 2;
