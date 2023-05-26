-- -- SELECT *
-- -- FROM orders_table
-- -- DO $$
-- -- DECLARE 
-- --     product_len INTEGER;
-- --     store_len INTEGER;
-- --     alter_statement TEXT;
-- -- BEGIN
-- --     SELECT 
-- --         -- MAX(LENGTH(product_code)) INTO product_len,
-- --         MAX(LENGTH(store_code)) INTO store_len 
-- --     FROM orders_table;
-- --     alter_statement := '
-- --                     ALTER TABLE orders_table ALTER COLUMN card_number 
-- --                         TYPE VARCHAR(' || product_len || ');';
-- --     EXECUTE alter_statement;
-- -- END $$

-- DECLARE product_len INT;
-- BEGIN
--     SELECT MAX(LENGTH(product_code)) INTO product_len
-- FROM
--     orders_table;

-- ALTER TABLE orders_table
--     ALTER COLUMN product_code TYPE VARCHAR(product_len)

-- -- DO $$
-- -- DECLARE 
-- --     product_len INTEGER;
-- --     alter_statement TEXT;
-- -- BEGIN
-- --     SELECT 
-- --         MAX(LENGTH(product_code)) INTO product_len FROM orders_table;

-- --     ALTER TABLE orders_table
-- --         ALTER COLUMN product_code TYPE VARCHAR(11);
-- --     -- alter_statement := 'ALTER TABLE orders_table ALTER COLUMN product_code 
-- --     --                     TYPE VARCHAR(' || product_len || ');';
-- --     -- EXECUTE alter_statement
-- -- END $$

-- Casting the orders table -- Task 1
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(255),
    ALTER COLUMN product_code TYPE VARCHAR(255),
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Casting the Users table -- Task 2
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE,
    ALTER COLUMN country_code TYPE VARCHAR(255),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    ALTER COLUMN join_date TYPE DATE;

--  Task 3 -- Casting the Store table
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(255),
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    -- ALTER COLUMN store_type TYPE VARCHAR(255) NULLABLE,
    ALTER COLUMN latitude TYPE FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(255),
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Task 4 -- Updating products table
-- Adding col to table
ALTER TABLE dim_products
    ADD COLUMN IF NOT EXISTS weight_class VARCHAR(255);
-- Updating col values
UPDATE dim_products
        SET weight_class = (CASE
                                WHEN weight < '2' THEN 'Light'
                                WHEN weight BETWEEN '2' AND '39.999' THEN 'Mid_Sized'
                                WHEN weight BETWEEN '40' AND '139.999' THEN 'Heavy'
                                WHEN weight >= '140' THEN 'Truck_Required'
                            END)
            -- still_available = (CASE
            --                         WHEN still_available = 'Still_available' THEN 'TRUE'
            --                         WHEN still_available = 'Removed' THEN 'FALSE')
                                    ;

-- Task 5 -- Casting product table
-- Rename column
DO $$
BEGIN
IF EXISTS(SELECT *
    FROM information_schema.columns
    WHERE table_name='dim_products' and column_name='removed')
  THEN
      ALTER TABLE "public"."dim_products" RENAME COLUMN "removed" TO "still_available";
  END IF;
END $$;

-- Cast the datatypes
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(255),
    ALTER COLUMN product_code TYPE VARCHAR(255),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    -- ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
    ALTER COLUMN weight_class TYPE VARCHAR(255);

-- --  Task 6 -- Cast the date table
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(255),
    ALTER COLUMN year TYPE VARCHAR(255),
    ALTER COLUMN day TYPE VARCHAR(255),
    ALTER COLUMN time_period TYPE VARCHAR(255),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Task 7 -- Cast Card table
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(255),
    ALTER COLUMN expiry_date TYPE VARCHAR(255),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

--  Task 8 -- ADD Primary Keys
-- ALTER TABLE dim_card_details
--     ADD CONSTRAINT card_number_pk PRIMARY KEY (card_number);
-- ALTER TABLE dim_date_times
    -- ADD CONSTRAINT date_uuid_pk PRIMARY KEY (date_uuid);
-- ALTER TABLE dim_products
--     ADD CONSTRAINT product_code_pk PRIMARY KEY (product_code);
-- ALTER TABLE dim_store_details
--     ADD CONSTRAINT store_code_pk PRIMARY KEY (store_code);
-- ALTER TABLE dim_users
--     ADD CONSTRAINT user_uuid_pk PRIMARY KEY (user_uuid);


-- Task 9 -- Add Foreign Key
ALTER TABLE orders_table
    -- ADD CONSTRAINT fk_order_card FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number),
    -- ADD CONSTRAINT fk_order_dt FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid),
    -- ADD CONSTRAINT fk_order_product FOREIGN KEY (product_code) REFERENCES dim_products (product_code),
    -- ADD CONSTRAINT fk_order_stores FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code),
    -- ADD CONSTRAINT fk_order_users FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);
