--  Task 8 -- ADD Primary Keys
ALTER TABLE dim_card_details
    ADD CONSTRAINT card_number_pk PRIMARY KEY (card_number);
ALTER TABLE dim_date_times
    ADD CONSTRAINT date_uuid_pk PRIMARY KEY (date_uuid);
ALTER TABLE dim_products
    ADD CONSTRAINT product_code_pk PRIMARY KEY (product_code);
ALTER TABLE dim_store_details
    ADD CONSTRAINT store_code_pk PRIMARY KEY (store_code);
ALTER TABLE dim_users
    ADD CONSTRAINT user_uuid_pk PRIMARY KEY (user_uuid);