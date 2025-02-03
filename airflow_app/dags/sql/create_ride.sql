CREATE TABLE IF NOT EXISTS ride (
    ride_id SERIAL PRIMARY KEY,
    cust_id INTEGER NOT NULL,
    driver_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    distance_km DECIMAL (5,2),  -- contoh 999.99 km
    fare DECIMAL(10,2), --contoh 99,999,999.99 RP
    ride_status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    FOREIGN KEY (cust_id) REFERENCES customer(cust_id),
    FOREIGN KEY (driver_id) REFERENCES driver(driver_id)
);