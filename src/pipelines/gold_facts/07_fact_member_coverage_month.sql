
CREATE OR REFRESH MATERIALIZED VIEW fact_member_coverage_month
COMMENT 'Monthly member coverage days and member-month equivalents'
AS
WITH coverage_source AS (
    SELECT
        coverage_id,
        member_id,
        subscription_id,
        benefit_plan_id,
        coverage_status,
        coverage_revision,
        coverage_start_date,

        LEAST(
            COALESCE(coverage_end_date, DATE '2999-12-31'),
            CURRENT_DATE()
        ) AS reporting_end_date

    FROM ${source_catalog}.silver.coverage
    WHERE NOT COALESCE(is_deleted, FALSE)
),

valid_periods AS (
    SELECT *
    FROM coverage_source
    WHERE coverage_start_date IS NOT NULL
      AND reporting_end_date >= coverage_start_date
),

expanded AS (
    SELECT
        c.*,
        EXPLODE(
            SEQUENCE(
                TRUNC(c.coverage_start_date, 'MM'),
                TRUNC(c.reporting_end_date, 'MM'),
                INTERVAL 1 MONTH
            )
        ) AS month_start_date

    FROM valid_periods c
),

monthly_slices AS (
    SELECT
        e.*,

        GREATEST(
            e.coverage_start_date,
            e.month_start_date
        ) AS covered_start_date,

        LEAST(
            e.reporting_end_date,
            LAST_DAY(e.month_start_date)
        ) AS covered_end_date

    FROM expanded e
),

dimension_matches AS (
    SELECT
        c.coverage_id,
        c.member_id,
        c.month_start_date,

        COUNT(DISTINCT p.plan_key) AS plan_matches,
        COUNT(DISTINCT s.subscription_key) AS subscription_matches,
        COUNT(DISTINCT m.member_key) AS member_matches,

        CASE WHEN COUNT(DISTINCT p.plan_key) = 1
            THEN MAX(p.plan_key) END AS plan_key,

        CASE WHEN COUNT(DISTINCT s.subscription_key) = 1
            THEN MAX(s.subscription_key) END AS subscription_key,

        CASE WHEN COUNT(DISTINCT m.member_key) = 1
            THEN MAX(m.member_key) END AS member_key

    FROM monthly_slices c

    LEFT JOIN ${source_catalog}.gold.dim_plan p
        ON c.benefit_plan_id = p.benefit_plan_id_bk
       AND c.covered_end_date BETWEEN
           p.effective_start_date AND p.effective_end_date
       AND NOT p.is_deleted

    LEFT JOIN ${source_catalog}.gold.dim_subscription s
        ON c.subscription_id = s.subscription_id_bk
       AND c.covered_end_date BETWEEN
           s.effective_start_date AND s.effective_end_date
       AND NOT s.is_deleted

    LEFT JOIN ${source_catalog}.gold.dim_member m
        ON c.member_id = m.member_id_bk
       AND m.is_current

    GROUP BY
        c.coverage_id, c.member_id, c.month_start_date
)

SELECT
    SHA2(
        CONCAT_WS(
            '|', 'COVERAGE_MONTH',
            c.member_id,
            c.coverage_id,
            CAST(c.month_start_date AS STRING)
        ), 256
    ) AS coverage_month_key,

    c.member_id AS member_id_bk,
    c.coverage_id AS coverage_id_bk,
    c.subscription_id AS subscription_id_bk,
    c.benefit_plan_id AS benefit_plan_id_bk,

    k.member_key,
    k.plan_key,
    k.subscription_key,

    c.coverage_status,
    c.coverage_revision,

    c.month_start_date,
    d.date_key AS month_key,

    c.covered_start_date,
    c.covered_end_date,

    DATEDIFF(
        c.covered_end_date,
        c.covered_start_date
    ) + 1 AS covered_days,

    DAY(LAST_DAY(c.month_start_date))
        AS days_in_month,

    CAST(
        ROUND(
            (
                DATEDIFF(
                    c.covered_end_date,
                    c.covered_start_date
                ) + 1
            ) / DAY(LAST_DAY(c.month_start_date)),
            6
        ) AS DECIMAL(12,6)
    ) AS member_month_equivalent,

    k.member_matches,
    k.plan_matches,
    k.subscription_matches,

    CASE
        WHEN k.member_matches > 1
          OR k.plan_matches > 1
          OR k.subscription_matches > 1
            THEN 'AMBIGUOUS_DIMENSION'
        WHEN k.member_matches = 0
          OR k.plan_matches = 0
          OR k.subscription_matches = 0
            THEN 'MISSING_DIMENSION'
        ELSE 'RESOLVED'
    END AS coverage_month_resolution_status,

    'MONTH_END_PLAN_AND_SUBSCRIPTION'
        AS dimension_date_basis

FROM monthly_slices c

LEFT JOIN dimension_matches k
    ON c.coverage_id = k.coverage_id
   AND c.member_id = k.member_id
   AND c.month_start_date = k.month_start_date

LEFT JOIN ${source_catalog}.gold.dim_date d
    ON c.month_start_date = d.calendar_date;
