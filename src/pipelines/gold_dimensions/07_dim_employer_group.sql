
CREATE OR REFRESH MATERIALIZED VIEW dim_employer_group
COMMENT 'Historical Employer Group Dimension'
AS
SELECT
    SHA2(CONCAT_WS('|',
        'EMPLOYER_GROUP', employer_group_id, TO_JSON(__START_AT)
    ), 256) AS employer_group_key,

    employer_group_id AS employer_group_id_bk,
    group_code,
    group_name,
    group_type,
    industry_category,
    sponsor_state,
    group_status,

    effective_start_date,
    effective_end_date,

    __START_AT AS version_start,
    __END_AT AS version_end,
    _source_batch_id,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.employer_group;
