-- Seed SQL script to create tables and import data from CSV, JSON, and Parquet

-- Create people table from CSV
CREATE TABLE people (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    notes VARCHAR
);

COPY people FROM 'test/data/people.csv' (DELIMITER ',', HEADER TRUE);

-- Create sales table from JSON
CREATE TABLE sales (
    sale_id INTEGER,
    date DATE,
    customer JSON,
    items JSON,
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR,
    status VARCHAR
);

COPY sales FROM 'test/data/sales.json' (FORMAT JSON);

-- Create trips table from Parquet
CREATE TABLE trips AS SELECT * FROM read_parquet('test/data/trips.parquet');

