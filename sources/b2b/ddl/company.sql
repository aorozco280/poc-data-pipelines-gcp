CREATE TABLE IF NOT EXISTS company (
    cuit INT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    is_supplier BOOLEAN NOT NULL
);
