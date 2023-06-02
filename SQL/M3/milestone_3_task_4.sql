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
                            END),
            removed = (CASE
                                    WHEN removed = 'Still_available' THEN 'TRUE'
                                    WHEN removed = 'Removed' THEN 'FALSE'
                                END);