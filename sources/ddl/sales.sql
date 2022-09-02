CREATE TABLE IF NOT EXISTS sales (
    order_id BIGINT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price_per_unit NUMERIC(10,2) NOT NULL,
    total NUMERIC(10,2) NOT NULL
);
