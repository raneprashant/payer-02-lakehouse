
CREATE OR REFRESH MATERIALIZED VIEW agg_denial_rate
COMMENT 'Claim-line denial rate by adjudication month and reason'
AS
WITH finalized AS (
    SELECT
        TRUNC(adjudication_date, 'MM') AS adjudication_month,
        plan_key,
        UPPER(TRIM(adjudication_status)) AS adjudication_status,
        denial_reason_code
    FROM ${source_catalog}.gold.fact_claim_line
    WHERE adjudication_date IS NOT NULL
      AND UPPER(TRIM(adjudication_status)) IN (
          'APPROVED', 'ADJUDICATED', 'DENIED', 'PAID'
      )
),
totals AS (
    SELECT
        adjudication_month,
        plan_key,
        COUNT(*) AS finalized_lines,
        COUNT_IF(adjudication_status = 'DENIED') AS denied_lines
    FROM finalized
    GROUP BY adjudication_month, plan_key
),
reasons AS (
    SELECT
        adjudication_month,
        plan_key,
        COALESCE(denial_reason_code, 'UNSPECIFIED')
            AS denial_reason_code,
        COUNT(*) AS denied_lines_for_reason
    FROM finalized
    WHERE adjudication_status = 'DENIED'
    GROUP BY
        adjudication_month,
        plan_key,
        COALESCE(denial_reason_code, 'UNSPECIFIED')
)
SELECT
    t.adjudication_month,
    t.plan_key,
    COALESCE(r.denial_reason_code, 'NO_DENIALS')
        AS denial_reason_code,

    t.finalized_lines,
    t.denied_lines,
    COALESCE(r.denied_lines_for_reason, 0)
        AS denied_lines_for_reason,

    ROUND(
        t.denied_lines * 100.0 / NULLIF(t.finalized_lines, 0),
        2
    ) AS overall_denial_rate_pct,

    ROUND(
        COALESCE(r.denied_lines_for_reason, 0) * 100.0
        / NULLIF(t.finalized_lines, 0),
        2
    ) AS reason_denial_rate_pct

FROM totals t
LEFT JOIN reasons r
    ON t.adjudication_month = r.adjudication_month
   AND t.plan_key <=> r.plan_key;
