
CREATE OR REFRESH MATERIALIZED VIEW dim_practitioner
COMMENT 'Historical Practitioner Dimension'
AS
SELECT
    SHA2(CONCAT_WS('|',
        'PRACTITIONER', practitioner_id, TO_JSON(__START_AT)
    ), 256) AS practitioner_key,

    practitioner_id AS practitioner_id_bk,
    individual_npi,
    first_name,
    last_name,
    CONCAT_WS(' ', first_name, last_name) AS practitioner_name,
    credential_code,
    practitioner_status,

    effective_start_date,
    effective_end_date,

    __START_AT AS version_start,
    __END_AT AS version_end,
    _source_batch_id,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.practitioner;
