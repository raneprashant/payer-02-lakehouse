
CREATE OR REFRESH MATERIALIZED VIEW dim_plan
COMMENT 'Gold historical Benefit Plan and Product dimension'
AS
SELECT
    SHA2(
        CONCAT_WS('|',
            'PLAN',
            b.benefit_plan_id,
            TO_JSON(b.__START_AT),
            COALESCE(TO_JSON(p.__START_AT), 'NO_PRODUCT')
        ), 256
    ) AS plan_key,

    b.benefit_plan_id AS benefit_plan_id_bk,
    b.product_id AS product_id_bk,

    b.plan_code,
    b.plan_name,
    b.coverage_type,
    b.plan_status,

    p.product_code,
    p.product_name,
    p.product_type,
    p.line_of_business,

    b.deductible_individual,
    b.deductible_family,
    b.oop_max_individual,
    b.oop_max_family,
    b.copay_primary_care,
    b.copay_specialist,
    b.coinsurance_pct,

    GREATEST(
        b.effective_start_date,
        COALESCE(p.effective_start_date, b.effective_start_date)
    ) AS effective_start_date,

    LEAST(
        COALESCE(b.effective_end_date, DATE '2999-12-31'),
        COALESCE(p.effective_end_date, DATE '2999-12-31')
    ) AS effective_end_date,

    b.__START_AT AS plan_version_start,
    b.__END_AT AS plan_version_end,
    p.__START_AT AS product_version_start,

    b._source_record_version AS plan_source_revision,
    b._source_sequence AS plan_source_sequence,

    (
        b.__END_AT IS NULL
        AND (p.__END_AT IS NULL OR p.product_id IS NULL)
        AND NOT b.is_deleted
        AND COALESCE(NOT p.is_deleted, TRUE)
    ) AS is_current,

    b.is_deleted

FROM ${source_catalog}.silver.benefit_plan b

LEFT JOIN ${source_catalog}.silver.product p
    ON b.product_id = p.product_id
   AND b.effective_start_date <=
       COALESCE(p.effective_end_date, DATE '2999-12-31')
   AND p.effective_start_date <=
       COALESCE(b.effective_end_date, DATE '2999-12-31');
