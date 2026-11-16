
CREATE OR REFRESH MATERIALIZED VIEW agg_claim_turnaround_monthly
COMMENT 'Claim turnaround and reopen counts by received month'
AS
WITH claims AS (
    SELECT
        claim_id,
        MIN(received_date) AS received_date,
        MAX(adjudication_date) AS adjudication_date,
        MAX(claim_type) AS claim_type,

        CASE WHEN COUNT(DISTINCT plan_key) = 1
            THEN MAX(plan_key)
        END AS plan_key

    FROM ${source_catalog}.gold.fact_claim_line
    GROUP BY claim_id
),
status_summary AS (
    SELECT
        claim_id,
        MAX_BY(status_code, source_sequence)
            AS latest_status,
        COUNT_IF(UPPER(status_code) = 'REOPENED')
            AS reopen_events
    FROM ${source_catalog}.gold.fact_claim_status_event
    GROUP BY claim_id
),
claim_metrics AS (
    SELECT
        TRUNC(c.received_date, 'MM') AS received_month,
        c.plan_key,
        c.claim_type,
        c.claim_id,

        COALESCE(s.reopen_events, 0) AS reopen_events,

        CASE
            WHEN UPPER(s.latest_status) IN (
                'ADJUDICATED', 'DENIED', 'CLOSED'
            )
             AND c.adjudication_date >= c.received_date
            THEN DATEDIFF(
                c.adjudication_date, c.received_date
            )
        END AS turnaround_days

    FROM claims c
    LEFT JOIN status_summary s
        ON c.claim_id = s.claim_id
)
SELECT
    received_month,
    plan_key,
    claim_type,

    COUNT(*) AS claims_received,
    COUNT(turnaround_days) AS finalized_claims,

    COUNT_IF(reopen_events > 0) AS reopened_claims,
    SUM(reopen_events) AS reopen_events,

    ROUND(AVG(turnaround_days), 2)
        AS average_turnaround_days,

    PERCENTILE_APPROX(turnaround_days, 0.50)
        AS median_turnaround_days,

    PERCENTILE_APPROX(turnaround_days, 0.90)
        AS p90_turnaround_days

FROM claim_metrics
WHERE received_month IS NOT NULL

GROUP BY
    received_month,
    plan_key,
    claim_type;
