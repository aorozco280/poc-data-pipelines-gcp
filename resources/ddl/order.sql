CREATE TABLE IF NOT EXISTS "order" (
    id BIGINT NOT NULL UNIQUE,
    order_datetime TIMESTAMP NOT NULL,
    customer_document_number INT NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL
);
