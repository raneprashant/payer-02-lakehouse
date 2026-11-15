
CREATE OR REFRESH MATERIALIZED VIEW dim_supplier_location
COMMENT 'Supplier location with effective service-address context'
AS
WITH site_versions AS (
    SELECT
        l.supplier_location_id,
        l.supplier_id,
        l.location_name,
        l.location_type,
        l.facility_npi,
        l.location_status,

        l.__START_AT AS location_version_start,
        l.__END_AT AS location_version_end,
        l.is_deleted,

        ea.entity_address_id,
        ea.address_id,

        a.address_line_1,
        a.city,
        a.state_code,
        a.postal_code,
        a.country_code,

        GREATEST(
            l.effective_start_date,
            COALESCE(ea.effective_start_date, l.effective_start_date)
        ) AS reporting_start,

        LEAST(
            COALESCE(l.effective_end_date, DATE '2999-12-31'),
            COALESCE(ea.effective_end_date, DATE '2999-12-31')
        ) AS reporting_end

    FROM ${source_catalog}.silver.supplier_location l

    LEFT JOIN ${source_catalog}.silver.entity_address ea
        ON ea.entity_id = l.supplier_location_id
       AND UPPER(ea.entity_type) = 'SUPPLIER_LOCATION'
       AND UPPER(ea.address_type) = 'SERVICE_LOCATION'
       AND ea.__END_AT IS NULL
       AND NOT COALESCE(ea.is_deleted, FALSE)
       AND ea.effective_start_date <=
           COALESCE(l.effective_end_date, DATE '2999-12-31')
       AND COALESCE(ea.effective_end_date, DATE '2999-12-31') >=
           l.effective_start_date

    LEFT JOIN ${source_catalog}.silver.address_master a
        ON ea.address_id = a.address_id
       AND a.__END_AT IS NULL
       AND NOT COALESCE(a.is_deleted, FALSE)
)
SELECT
    SHA2(CONCAT_WS('|',
        'LOCATION',
        supplier_location_id,
        TO_JSON(location_version_start),
        COALESCE(entity_address_id, 'NO_ADDRESS')
    ), 256) AS supplier_location_key,

    supplier_location_id AS supplier_location_id_bk,
    supplier_id AS supplier_id_bk,

    location_name,
    location_type,
    facility_npi,
    location_status,

    entity_address_id AS entity_address_id_bk,
    address_id AS address_id_bk,

    address_line_1,
    city,
    state_code,
    postal_code,
    country_code,

    CASE
        WHEN address_id IS NOT NULL THEN
            SHA2(CONCAT_WS('|',
                'GEO',
                COALESCE(UPPER(TRIM(country_code)), 'UNKNOWN'),
                COALESCE(UPPER(TRIM(state_code)), 'UNKNOWN'),
                COALESCE(UPPER(TRIM(city)), 'UNKNOWN'),
                COALESCE(TRIM(postal_code), 'UNKNOWN')
            ), 256)
    END AS geography_key,

    reporting_start AS effective_start_date,
    reporting_end AS effective_end_date,

    location_version_start AS version_start,
    location_version_end AS version_end,

    (location_version_end IS NULL AND NOT is_deleted)
        AS is_current,
    is_deleted

FROM site_versions
WHERE reporting_start <= reporting_end;
