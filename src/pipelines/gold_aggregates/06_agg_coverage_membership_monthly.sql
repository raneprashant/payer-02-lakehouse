
CREATE OR REFRESH MATERIALIZED VIEW agg_coverage_membership_monthly
COMMENT 'Covered days and member-months by plan, market and enrollment tier'
AS
WITH monthly_segments AS (
    SELECT
        c.month_start_date AS coverage_month,
        c.benefit_plan_id_bk,
        c.plan_key,
        COALESCE(s.coverage_market, 'UNKNOWN')
            AS coverage_market,
        COALESCE(e.tier_code, 'UNKNOWN') AS tier_code,
        c.member_id_bk,
        c.days_in_month,

        GREATEST(
            c.covered_start_date,
            e.enrollment_start_date
        ) AS segment_start,

        LEAST(
            c.covered_end_date,
            e.enrollment_end_date
        ) AS segment_end

    FROM ${source_catalog}.gold.fact_member_coverage_month c

    JOIN ${source_catalog}.silver.member_eligibility e
        ON c.coverage_id_bk = e.coverage_id
       AND c.member_id_bk = e.member_id
       AND c.covered_start_date <= e.enrollment_end_date
       AND c.covered_end_date >= e.enrollment_start_date

    LEFT JOIN ${source_catalog}.gold.dim_subscription s
        ON c.subscription_key = s.subscription_key
),
valid_slices AS (
    SELECT
        *,
        DATEDIFF(segment_end, segment_start) + 1
            AS segment_covered_days
    FROM monthly_segments
    WHERE segment_start <= segment_end
)
SELECT
    coverage_month,
    benefit_plan_id_bk,
    plan_key,
    coverage_market,
    tier_code,

    COUNT(DISTINCT member_id_bk) AS covered_members,
    SUM(segment_covered_days) AS covered_days,

    ROUND(
        SUM(segment_covered_days * 1.0 / days_in_month),
        6
    ) AS member_months

FROM valid_slices
GROUP BY
    coverage_month,
    benefit_plan_id_bk,
    plan_key,
    coverage_market,
    tier_code;
