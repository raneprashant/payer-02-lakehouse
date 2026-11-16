
CREATE OR REFRESH MATERIALIZED VIEW agg_claim_cost_monthly
COMMENT 'Monthly incurred claim cost by plan and provider'
AS
SELECT
    TRUNC(f.service_date, 'MM') AS service_month,
    f.plan_key,
    f.benefit_plan_id AS benefit_plan_id_bk,
    f.provider_key,
    f.supplier_key,
    MAX(p.line_of_business) AS line_of_business,

    COUNT(*) AS claim_lines,
    COUNT(DISTINCT f.claim_id) AS distinct_claims,
    SUM(f.billed_amount) AS billed_amount,
    SUM(f.net_allowed_amount) AS net_allowed_amount,
    SUM(f.net_payer_liability) AS net_payer_liability,
    SUM(f.member_responsibility_amount) AS member_responsibility_amount
FROM ${source_catalog}.gold.fact_claim_line f
LEFT JOIN ${source_catalog}.gold.dim_plan p
    ON f.plan_key = p.plan_key
WHERE f.service_date IS NOT NULL
GROUP BY
    TRUNC(f.service_date, 'MM'),
    f.plan_key,
    f.benefit_plan_id,
    f.provider_key,
    f.supplier_key;
