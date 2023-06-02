-- Casting the orders table -- Task 1
--  Changing the data types for the orders_table
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(255),
    ALTER COLUMN product_code TYPE VARCHAR(255),
    ALTER COLUMN product_quantity TYPE SMALLINT;