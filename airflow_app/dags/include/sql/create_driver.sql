CREATE TABLE IF NOT EXISTS driver (
    driver_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(50),
    phone_number INT NOT NULL,
    created_at TIMESTAMP NOT NULL
);