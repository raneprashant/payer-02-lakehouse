
CREATE OR REFRESH MATERIALIZED VIEW dim_specialty
COMMENT 'Medical Specialty Dimension'
AS
SELECT
    SHA2(CONCAT_WS('|',
        'SPECIALTY', specialty_id
    ), 256) AS specialty_key,

    specialty_id AS specialty_id_bk,
    specialty_code,
    specialty_name,
    specialty_category,
    specialty_status,

    _source_record_version AS source_revision,
    _source_sequence AS source_sequence,
    _source_batch_id,

    NOT COALESCE(is_deleted, FALSE) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.specialty;
