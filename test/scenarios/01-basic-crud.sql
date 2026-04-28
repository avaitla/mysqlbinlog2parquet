-- Basic CRUD across two tables. Demonstrates that the converter splits
-- the output Parquet by source table (one file per <db>.<table>).
CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sku VARCHAR(64) NOT NULL,
    qty INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob',   'bob@example.com');

INSERT INTO items (sku, qty) VALUES
    ('sku-0001', 10),
    ('sku-0002', 25),
    ('sku-0003', 5);

UPDATE items SET qty = 11 WHERE sku = 'sku-0001';
UPDATE customers SET email = 'alice+new@example.com' WHERE name = 'Alice';
DELETE FROM items WHERE sku = 'sku-0002';
