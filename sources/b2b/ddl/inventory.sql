CREATE TABLE IF NOT EXISTS inventory (
    company_cuit INT NOT NULL,
    product_id INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    stock_count INT NOT NULL
);
