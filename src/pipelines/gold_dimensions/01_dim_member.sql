
CREATE OR REFRESH MATERIALIZED VIEW dim_member
COMMENT 'Gold Member Dimension - historical member versions'
AS
SELECT
    SHA2(
        CONCAT_WS('|',
            member_id,
            TO_JSON(__START_AT)
        ),
        256
    ) AS member_key,

    member_id AS member_id_bk,
    first_name,
    last_name,
    CONCAT_WS(' ', first_name, last_name) AS member_name,
    birth_date,
    sex_code,
    member_status,
    household_id,
    death_date,

    __START_AT AS version_start,
    __END_AT AS version_end,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted,
    _source_batch_id

FROM ${source_catalog}.silver.member_master;
