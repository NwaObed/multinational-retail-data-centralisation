-- --  Task 6 -- Cast the date table
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(255),
    ALTER COLUMN year TYPE VARCHAR(255),
    ALTER COLUMN day TYPE VARCHAR(255),
    ALTER COLUMN time_period TYPE VARCHAR(255),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;