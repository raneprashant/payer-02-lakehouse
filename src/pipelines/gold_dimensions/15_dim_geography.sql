
CREATE OR REFRESH MATERIALIZED VIEW dim_geography
COMMENT 'Reusable reporting geography dimension'
AS
WITH normalized AS (
    SELECT
        address_id,

        COALESCE(
            UPPER(TRIM(country_code)), 'UNKNOWN'
        ) AS country_code,

        COALESCE(
            UPPER(TRIM(state_code)), 'UNKNOWN'
        ) AS state_code,

        COALESCE(
            UPPER(TRIM(city)), 'UNKNOWN'
        ) AS city,

        COALESCE(
            TRIM(postal_code), 'UNKNOWN'
        ) AS postal_code,

        county_name

    FROM ${source_catalog}.silver.address_master
)

SELECT
    SHA2(CONCAT_WS('|',
        'GEO',
        country_code,
        state_code,
        city,
        postal_code
    ), 256) AS geography_key,

    CONCAT_WS('|',
        country_code,
        state_code,
        city,
        postal_code
    ) AS geography_bk,

    country_code,
    state_code,
    city,
    postal_code,

    SORT_ARRAY(
        COLLECT_SET(county_name)
    ) AS county_names,

    COUNT(DISTINCT address_id) AS distinct_address_count

FROM normalized

GROUP BY
    country_code,
    state_code,
    city,
    postal_code;
