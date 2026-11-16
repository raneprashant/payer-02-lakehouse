
CREATE OR REFRESH MATERIALIZED VIEW agg_provider_network_utilization
COMMENT 'Provider utilization by service-date network participation'
AS
SELECT
    TRUNC(f.service_date, 'MM') AS service_month,
    f.plan_key,
    f.network_key,
    f.provider_key,
    f.supplier_key,

    COALESCE(
        UPPER(TRIM(e.participation_status)),
        'UNKNOWN'
    ) AS participation_status,

    COUNT(*) AS claim_lines,
    COUNT(DISTINCT f.claim_id) AS distinct_claims,
    SUM(f.units) AS service_units,
    SUM(f.net_allowed_amount) AS net_allowed_amount,
    SUM(f.net_payer_liability) AS net_payer_liability

FROM ${source_catalog}.gold.fact_claim_line f

LEFT JOIN ${source_catalog}.silver.claim_enriched e
    ON f.claim_id = e.claim_id
   AND f.line_number = e.line_number

WHERE f.service_date IS NOT NULL

GROUP BY
    TRUNC(f.service_date, 'MM'),
    f.plan_key,
    f.network_key,
    f.provider_key,
    f.supplier_key,
    COALESCE(
        UPPER(TRIM(e.participation_status)),
        'UNKNOWN'
    );
