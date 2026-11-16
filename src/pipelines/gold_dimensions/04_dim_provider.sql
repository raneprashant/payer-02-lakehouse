
CREATE OR REFRESH MATERIALIZED VIEW dim_provider
COMMENT 'Gold historical rendering provider role dimension'
AS
WITH provider_versions AS (
    SELECT
        r.*,
        p.__START_AT AS practitioner_version_start,
        p.__END_AT AS practitioner_version_end,
        p.first_name AS practitioner_first_name,
        p.last_name AS practitioner_last_name,
        p.individual_npi,
        p.credential_code,
        p.practitioner_status,
        p.is_deleted AS practitioner_deleted,

        l.__START_AT AS location_version_start,
        l.__END_AT AS location_version_end,
        l.supplier_id,
        l.location_name,
        l.location_type,
        l.facility_npi,
        l.location_status,
        l.is_deleted AS location_deleted,

        GREATEST(
            r.effective_start_date,
            COALESCE(p.effective_start_date, r.effective_start_date),
            COALESCE(l.effective_start_date, r.effective_start_date)
        ) AS reporting_start,

        LEAST(
            COALESCE(r.effective_end_date, DATE '2999-12-31'),
            COALESCE(p.effective_end_date, DATE '2999-12-31'),
            COALESCE(l.effective_end_date, DATE '2999-12-31')
        ) AS reporting_end

    FROM ${source_catalog}.silver.practitioner_role r

    LEFT JOIN ${source_catalog}.silver.practitioner p
        ON r.practitioner_id = p.practitioner_id
       AND r.effective_start_date <=
           COALESCE(p.effective_end_date, DATE '2999-12-31')
       AND p.effective_start_date <=
           COALESCE(r.effective_end_date, DATE '2999-12-31')

    LEFT JOIN ${source_catalog}.silver.supplier_location l
        ON r.supplier_location_id = l.supplier_location_id
       AND r.effective_start_date <=
           COALESCE(l.effective_end_date, DATE '2999-12-31')
       AND l.effective_start_date <=
           COALESCE(r.effective_end_date, DATE '2999-12-31')
)
SELECT
    SHA2(
        CONCAT_WS('|',
            'PROVIDER',
            practitioner_role_id,
            TO_JSON(__START_AT),
            COALESCE(TO_JSON(practitioner_version_start), 'NO_PRACTITIONER'),
            COALESCE(TO_JSON(location_version_start), 'NO_LOCATION')
        ), 256
    ) AS provider_key,

    practitioner_role_id AS practitioner_role_id_bk,
    practitioner_id AS practitioner_id_bk,
    supplier_location_id AS supplier_location_id_bk,
    supplier_id AS supplier_id_bk,

    practitioner_first_name,
    practitioner_last_name,
    individual_npi,
    credential_code,
    practitioner_status,

    role_code,
    role_status,
    specialty_id,

    location_name,
    location_type,
    facility_npi,
    location_status,

    reporting_start AS effective_start_date,
    reporting_end AS effective_end_date,

    __START_AT AS role_version_start,
    __END_AT AS role_version_end,
    practitioner_version_start,
    location_version_start,
    _source_record_version AS role_source_revision,
    _source_sequence AS role_source_sequence,

    (
        __END_AT IS NULL
        AND practitioner_version_end IS NULL
        AND location_version_end IS NULL
        AND NOT is_deleted
        AND COALESCE(NOT practitioner_deleted, FALSE)
        AND COALESCE(NOT location_deleted, FALSE)
    ) AS is_current,

    is_deleted

FROM provider_versions
WHERE reporting_start <= reporting_end;
