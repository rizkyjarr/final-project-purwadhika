CREATE TABLE IF NOT EXISTS ride (
    ride_id SERIAL PRIMARY KEY,
    cust_id INT REFERENCES customer(cust_id),
    driver_id INT REFERENCES driver(driver_id),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    distance_km DECIMAL (5,2),  -- contoh 999.99 km
    fare DECIMAL(10,2), --contoh 99,999,999.99 RP
    ride_status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL
);