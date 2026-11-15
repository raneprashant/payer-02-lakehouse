
CREATE OR REFRESH MATERIALIZED VIEW agg_pmpm
COMMENT 'Incurred payer cost per covered member-month'
AS
WITH costs AS (
    SELECT
        TRUNC(f.service_date, 'MM') AS reporting_month,
        f.benefit_plan_id AS benefit_plan_id_bk,
        COALESCE(s.coverage_market, 'UNKNOWN') AS coverage_market,
        SUM(f.net_payer_liability) AS incurred_cost,
        COUNT(*) AS claim_lines

    FROM ${source_catalog}.gold.fact_claim_line f
    LEFT JOIN ${source_catalog}.gold.dim_subscription s
        ON f.subscription_key = s.subscription_key
    WHERE f.service_date IS NOT NULL
    GROUP BY
        TRUNC(f.service_date, 'MM'),
        f.benefit_plan_id,
        COALESCE(s.coverage_market, 'UNKNOWN')
),
membership AS (
    SELECT
        c.month_start_date AS reporting_month,
        c.benefit_plan_id_bk,
        COALESCE(s.coverage_market, 'UNKNOWN') AS coverage_market,
        SUM(c.covered_days) AS covered_days,
        SUM(c.member_month_equivalent) AS member_months

    FROM ${source_catalog}.gold.fact_member_coverage_month c
    LEFT JOIN ${source_catalog}.gold.dim_subscription s
        ON c.subscription_key = s.subscription_key
    GROUP BY
        c.month_start_date,
        c.benefit_plan_id_bk,
        COALESCE(s.coverage_market, 'UNKNOWN')
),
cohorts AS (
    SELECT reporting_month, benefit_plan_id_bk, coverage_market
    FROM costs
    UNION
    SELECT reporting_month, benefit_plan_id_bk, coverage_market
    FROM membership
),
plan_details AS (
    SELECT
        benefit_plan_id_bk,
        MAX(line_of_business) AS line_of_business
    FROM ${source_catalog}.gold.dim_plan
    WHERE is_current
    GROUP BY benefit_plan_id_bk
)
SELECT
    c.reporting_month,
    c.benefit_plan_id_bk,
    p.line_of_business,
    c.coverage_market,

    COALESCE(co.incurred_cost, 0) AS incurred_cost,
    COALESCE(co.claim_lines, 0) AS claim_lines,
    COALESCE(m.covered_days, 0) AS covered_days,
    COALESCE(m.member_months, 0) AS member_months,

    CASE
        WHEN m.member_months > 0
        THEN ROUND(co.incurred_cost / m.member_months, 2)
    END AS pmpm,

    CASE
        WHEN m.member_months IS NULL OR m.member_months = 0
            THEN 'NO_MEMBER_MONTH_DENOMINATOR'
        ELSE 'CALCULATED'
    END AS pmpm_status

FROM cohorts c
LEFT JOIN costs co
    ON c.reporting_month = co.reporting_month
   AND c.benefit_plan_id_bk <=> co.benefit_plan_id_bk
   AND c.coverage_market = co.coverage_market
LEFT JOIN membership m
    ON c.reporting_month = m.reporting_month
   AND c.benefit_plan_id_bk <=> m.benefit_plan_id_bk
   AND c.coverage_market = m.coverage_market
LEFT JOIN plan_details p
    ON c.benefit_plan_id_bk = p.benefit_plan_id_bk;
