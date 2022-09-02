CREATE TABLE IF NOT EXISTS product (
    id INT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    supplier_cuit INT NOT NULL,
    default_price NUMERIC(10,2) NOT NULL
);
