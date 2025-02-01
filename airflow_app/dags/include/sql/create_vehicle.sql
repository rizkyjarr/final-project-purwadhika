CREATE TABLE IF NOT EXISTS vehicle (
    vehicle_id SERIAL PRIMARY KEY,
    driver_id INT REFERENCES driver(driver_id),
    vehicle_type TEXT NOT NULL,
    license_plate TEXT NOT NULL UNIQUE,
    year INT,
    brand TEXT,
    created_at TIMESTAMP NOT NULL    
);