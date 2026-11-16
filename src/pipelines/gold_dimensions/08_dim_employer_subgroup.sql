
CREATE OR REFRESH MATERIALIZED VIEW dim_employer_subgroup
COMMENT 'Historical Employer Subgroup Dimension'
AS
SELECT
    SHA2(CONCAT_WS('|',
        'EMPLOYER_SUBGROUP',
        employer_subgroup_id,
        TO_JSON(__START_AT)
    ), 256) AS employer_subgroup_key,

    employer_subgroup_id AS employer_subgroup_id_bk,
    employer_group_id AS employer_group_id_bk,

    subgroup_code,
    subgroup_name,
    subgroup_category,

    effective_start_date,
    effective_end_date,

    __START_AT AS version_start,
    __END_AT AS version_end,
    _source_batch_id,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.employer_subgroup;
