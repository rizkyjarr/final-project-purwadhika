CREATE TABLE IF NOT EXISTS vehicle (
    vehicle_id SERIAL PRIMARY KEY,
    driver_id INTEGER NOT NULL,
    vehicle_type VARCHAR(255),
    license_plate TEXT NOT NULL UNIQUE,
    year TEXT,
    brand TEXT,
    created_at TIMESTAMP NOT NULL,
    FOREIGN KEY (driver_id) REFERENCES driver(driver_id)  
);