-- Basic CRUD on a single table.
CREATE TABLE IF NOT EXISTS items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sku VARCHAR(64) NOT NULL,
    qty INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO items (sku, qty) VALUES
    ('sku-0001', 10),
    ('sku-0002', 25),
    ('sku-0003', 5);

UPDATE items SET qty = 11 WHERE sku = 'sku-0001';

DELETE FROM items WHERE sku = 'sku-0002';
