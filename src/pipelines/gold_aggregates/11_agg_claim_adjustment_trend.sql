
CREATE OR REFRESH MATERIALIZED VIEW agg_claim_adjustment_trend
COMMENT 'Signed adjustment trends by adjustment and service month'
AS
SELECT
    TRUNC(a.adjustment_effective_date, 'MM')
        AS adjustment_month,

    TRUNC(f.service_date, 'MM')
        AS original_service_month,

    a.plan_key,
    COALESCE(a.adjustment_reason_code, 'UNSPECIFIED')
        AS adjustment_reason_code,

    COUNT(*) AS adjustment_events,
    COUNT(DISTINCT a.claim_id) AS affected_claims,

    COUNT_IF(a.reversal_of_adjustment_id IS NOT NULL)
        AS reversal_events,

    SUM(a.allowed_amount_delta) AS allowed_delta,
    SUM(a.member_responsibility_delta) AS member_delta,
    SUM(a.payer_liability_delta) AS payer_liability_delta

FROM ${source_catalog}.gold.fact_claim_adjustment a

LEFT JOIN ${source_catalog}.gold.fact_claim_line f
    ON a.claim_line_key = f.claim_line_key

GROUP BY
    TRUNC(a.adjustment_effective_date, 'MM'),
    TRUNC(f.service_date, 'MM'),
    a.plan_key,
    COALESCE(a.adjustment_reason_code, 'UNSPECIFIED');
