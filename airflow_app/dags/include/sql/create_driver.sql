CREATE TABLE IF NOT EXISTS driver (
    driver_id BIGINT PRIMARY KEY,  -- will allow for python script to generate id, not using serial key
    name VARCHAR(255),
    email VARCHAR(50) UNIQUE,  -- needs to be ensured that email is unique
    phone_number VARCHAR(20) NOT NULL,  -- Storing as text to preserve formatting
    created_at TIMESTAMP NOT NULL
);