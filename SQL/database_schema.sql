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


















