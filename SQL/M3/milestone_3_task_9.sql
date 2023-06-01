-- Task 9 -- Add Foreign Key
ALTER TABLE orders_table
    ADD CONSTRAINT fk_order_card FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number),
    ADD CONSTRAINT fk_order_dt FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid),
    ADD CONSTRAINT fk_order_product FOREIGN KEY (product_code) REFERENCES dim_products (product_code),
    ADD CONSTRAINT fk_order_stores FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code),
    ADD CONSTRAINT fk_order_users FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);
