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