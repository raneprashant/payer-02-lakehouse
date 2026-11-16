
CREATE OR REFRESH MATERIALIZED VIEW agg_provider_contract_variance
COMMENT 'Contract-rate verification exposure; variance not yet measurable'
AS
SELECT
    TRUNC(service_date, 'MM') AS service_month,
    plan_key,
    provider_key,
    procedure_code,

    COUNT(*) AS claim_lines,

    COUNT_IF(contract_rate_id IS NOT NULL)
        AS candidate_rate_matches,

    COUNT_IF(resolution_status = 'POS_NOT_VERIFIED')
        AS pos_unverified_lines,

    COUNT_IF(contract_rate_id IS NULL)
        AS missing_contract_rate_lines,

    SUM(net_allowed_amount) AS net_allowed_amount,
    SUM(net_payer_liability) AS net_payer_liability,

    CAST(NULL AS DECIMAL(18,2))
        AS verified_contract_variance_amount,

    'RATE_BASIS_NOT_VERIFIED'
        AS variance_status

FROM ${source_catalog}.gold.fact_claim_line

WHERE service_date IS NOT NULL

GROUP BY
    TRUNC(service_date, 'MM'),
    plan_key,
    provider_key,
    procedure_code;
