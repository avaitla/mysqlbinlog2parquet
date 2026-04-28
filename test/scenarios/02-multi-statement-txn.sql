-- Multiple row events grouped under a single GTID.
CREATE TABLE IF NOT EXISTS accounts (
    id INT PRIMARY KEY,
    balance DECIMAL(12,2) NOT NULL
);

INSERT INTO accounts (id, balance) VALUES (1, 100.00), (2, 50.00);

START TRANSACTION;
UPDATE accounts SET balance = balance - 25.00 WHERE id = 1;
UPDATE accounts SET balance = balance + 25.00 WHERE id = 2;
COMMIT;
