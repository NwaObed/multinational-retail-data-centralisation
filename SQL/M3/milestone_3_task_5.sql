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
    ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
    ALTER COLUMN weight_class TYPE VARCHAR(255);