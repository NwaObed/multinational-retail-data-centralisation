--  Task 3 -- Casting the Store table
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(255),
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    -- ALTER COLUMN store_type TYPE VARCHAR(255) NULL,
    ALTER COLUMN latitude TYPE FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(255),
    ALTER COLUMN continent TYPE VARCHAR(255);