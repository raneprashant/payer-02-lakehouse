
CREATE OR REFRESH MATERIALIZED VIEW dim_date
COMMENT 'Gold calendar date dimension'
AS
WITH calendar AS (
    SELECT EXPLODE(
        SEQUENCE(
            DATE '2020-01-01',
            DATE '2040-12-31',
            INTERVAL 1 DAY
        )
    ) AS calendar_date
)
SELECT
    CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT)
        AS date_key,
    calendar_date,
    YEAR(calendar_date) AS calendar_year,
    QUARTER(calendar_date) AS calendar_quarter,
    MONTH(calendar_date) AS calendar_month,
    DAY(calendar_date) AS day_of_month,
    WEEKOFYEAR(calendar_date) AS week_of_year,
    DAYOFWEEK(calendar_date) AS day_of_week,
    DATE_TRUNC('MONTH', calendar_date)::DATE AS month_start_date,
    DATE_FORMAT(calendar_date, 'MMM-yyyy') AS month_label,
    CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7)
         THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar;
