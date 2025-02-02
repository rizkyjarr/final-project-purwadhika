CREATE TABLE IF NOT EXISTS customer (
    cust_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    phone_number VARCHAR(20) NOT NULL,
    email VARCHAR(50) UNIQUE,
    created_at TIMESTAMP NOT NULL
);