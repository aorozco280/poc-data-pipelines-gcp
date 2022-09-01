CREATE TABLE IF NOT EXISTS customer (
    document_number INT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    date_of_birth DATE NOT NULL,
    address TEXT NOT NULL,
    country TEXT NOT NULL
);
