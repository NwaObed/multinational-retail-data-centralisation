# Multinational Retail Data Centralisation

In this project, I extracted data from multiple sources and formats (`.pdf`, `.csv`, `.json`), built a centralised database with star-based schema to store the data, cleaned the data and uploaded the data to the database. The project is implemented in the `retail_project.py` file.

In the `Multinational_Retail` folder, the `data_extraction.py` module implements the `DataExtractor` class that extracts the data from multiple data sources. After extraction, the `data_cleaning.py` module cleans the data by checking for `NULL` values, data consistency, and data conversion. Finally, the `database_utils.py` module implements the `DatabaseConnector` class that connects and uploads the clean data to the centralised database.

Using the clean data uploaded to the database, I queried the database using `SQL` and answered some important questions. For example, using the following code snippets, I calculated the total number number of sales and product quantity count coming from online `vs` offline.

```SQL
-- TAsk 4 : : Get the total sales online vs offline
SELECT 
	COUNT(store_type) numbers_of_sales,
	SUM(product_quantity) AS product_quantity_count,
    CASE
		-- Group the stores into two categories - online and offline
        WHEN store_type = 'Web Portal' THEN 'Web'
        WHEN store_type IN ('Mall Kiosk', 'Super Store', 'Local', 'Outlet') THEN 'Offline'
    END AS location
FROM dim_store_details
-- Join the different tables using the PK and FK
JOIN
	orders_table
ON 
	dim_store_details.store_code = orders_table.store_code
JOIN 
	dim_products
ON
	dim_products.product_code = orders_table.product_code
GROUP BY
	location
ORDER BY 
	product_quantity_count
```

Additionally, I also calculated the average time taken between each sales grouped by year using the codes below.
```SQL
WITH timestamp_table AS(
-- 	Create a temporary table for the timestamp using the year, month, day and timestamp column
	SELECT
		to_timestamp(year || '-' || month || '-' || day || '-' || timestamp, 'YYYY-MM-DD HH24:MI:SS')::timestamp  AS concat_timestamp
	From dim_date_times
), extract_table AS (
-- 	Extract the hour, minute, and seconds from the temporary timestamp_table
	SELECT 
		concat_timestamp,
		EXTRACT(YEAR FROM concat_timestamp) AS year,
		EXTRACT(HOUR FROM concat_timestamp) AS hour,
		EXTRACT(MINUTE FROM concat_timestamp) AS minute,
		EXTRACT(SECOND FROM concat_timestamp) AS second
	FROM timestamp_table
), extract_lead_table AS (
	SELECT
-- 	Compute the metrics using the LEAD function
		year,
		hour,
		LEAD(hour, 1) OVER (PARTITION BY year) AS actual_hour_taken,
		LEAD(minute, 1) OVER (PARTITION BY year) AS actual_minute_taken,
		LEAD(second, 1) OVER (PARTITION BY year) AS actual_second_taken
	FROM
		extract_table
)
SELECT
-- Compute the average time grouped over the years
	year,
	'"hours": '|| ROUND(AVG(actual_hour_taken)) || ' "minutes": '|| ROUND(AVG(actual_minute_taken)) || ' "seconds": ' || ROUND(AVG(actual_second_taken)) || ' "millise..."' AS actual_time_taken
FROM
	extract_lead_table
GROUP BY
	year
```
