
CREATE OR REFRESH MATERIALIZED VIEW agg_retro_eligibility_impact
COMMENT 'Potential claim exposure associated with eligibility audit events'
AS
WITH audit_coverage AS (
    SELECT
        TRUNC(CAST(change_processed_at AS DATE), 'MM')
            AS processing_month,
        coverage_id,

        COUNT(DISTINCT change_audit_id)
            AS audit_event_count

    FROM ${source_catalog}.silver.eligibility_change_audit
    WHERE coverage_id IS NOT NULL
    GROUP BY
        TRUNC(CAST(change_processed_at AS DATE), 'MM'),
        coverage_id
),
claim_cohorts AS (
    SELECT
        coverage_id,
        TRUNC(service_date, 'MM') AS service_month,
        plan_key,

        COUNT(*) AS claim_lines,
        SUM(net_payer_liability)
            AS current_liability_exposure

    FROM ${source_catalog}.gold.fact_claim_line
    WHERE coverage_id IS NOT NULL
      AND service_date IS NOT NULL

    GROUP BY
        coverage_id,
        TRUNC(service_date, 'MM'),
        plan_key
)
SELECT
    a.processing_month,
    c.service_month,
    c.plan_key,

    SUM(a.audit_event_count)
        AS audit_events_in_cohort,

    SUM(COALESCE(c.claim_lines, 0))
        AS potentially_affected_claim_lines,

    SUM(COALESCE(c.current_liability_exposure, 0))
        AS current_liability_exposure,

    CAST(NULL AS BIGINT)
        AS confirmed_eligibility_flips,

    CAST(NULL AS DECIMAL(18,2))
        AS confirmed_liability_change,

    'POTENTIAL_EXPOSURE_ONLY'
        AS impact_measurement_status

FROM audit_coverage a

LEFT JOIN claim_cohorts c
    ON a.coverage_id = c.coverage_id

GROUP BY
    a.processing_month,
    c.service_month,
    c.plan_key;
