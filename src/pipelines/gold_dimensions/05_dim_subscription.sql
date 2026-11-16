
CREATE OR REFRESH MATERIALIZED VIEW dim_subscription
COMMENT 'Gold historical subscription dimension'
AS
SELECT
    SHA2(
        CONCAT_WS('|',
            subscription_id,
            TO_JSON(__START_AT)
        ), 256
    ) AS subscription_key,

    subscription_id AS subscription_id_bk,
    subscriber_member_id AS subscriber_member_id_bk,

    coverage_market,
    funding_arrangement,
    employer_group_id AS employer_group_id_bk,
    employer_subgroup_id AS employer_subgroup_id_bk,
    subscription_status,

    subscription_start_date AS effective_start_date,
    subscription_end_date AS effective_end_date,

    __START_AT AS version_start,
    __END_AT AS version_end,

    _source_record_version AS source_revision,
    _source_sequence AS source_sequence,
    _source_batch_id,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.subscription;
