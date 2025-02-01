CREATE TABLE IF NOT EXISTS customer (
    cust_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    phone_number INT NOT NULL,
    email VARCHAR(50),
    created_at TIMESTAMP NOT NULL
);