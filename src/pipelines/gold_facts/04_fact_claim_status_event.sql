
CREATE OR REFRESH MATERIALIZED VIEW fact_claim_status_event
COMMENT 'Claim lifecycle status event fact'
AS
WITH ordered_events AS (
    SELECT
        e.*,

        LAG(e.status_code) OVER (
            PARTITION BY e.claim_id
            ORDER BY e.source_sequence,
                     e.event_ts,
                     e.claim_status_event_id
        ) AS previous_status_code,

        LAG(e.status_effective_date) OVER (
            PARTITION BY e.claim_id
            ORDER BY e.source_sequence,
                     e.event_ts,
                     e.claim_status_event_id
        ) AS previous_status_date

    FROM ${source_catalog}.silver.claim_status_event e
)
SELECT
    SHA2(
        CONCAT_WS('|', 'CLAIM_STATUS', e.claim_status_event_id),
        256
    ) AS claim_status_event_key,

    e.claim_status_event_id,
    e.claim_id,
    h.member_id AS member_id_bk,
    m.member_key,

    e.status_code,
    e.previous_status_code,
    e.status_reason_code,
    e.source_sequence,

    e.status_effective_date,
    e.event_ts,
    e.previous_status_date,

    CASE
        WHEN e.previous_status_date IS NOT NULL
        THEN DATEDIFF(
            e.status_effective_date,
            e.previous_status_date
        )
    END AS days_since_previous_status,

    d.date_key AS status_date_key,
    td.date_key AS event_date_key,

    CASE
        WHEN h.claim_id IS NULL THEN 'MISSING_CLAIM_HEADER'
        WHEN e.previous_status_date > e.status_effective_date
            THEN 'NEGATIVE_STATUS_INTERVAL'
        ELSE 'RESOLVED'
    END AS status_resolution_status,

    'CURRENT_MEMBER_VERSION' AS member_key_resolution_method

FROM ordered_events e

LEFT JOIN ${source_catalog}.silver.claim_header h
    ON e.claim_id = h.claim_id
   AND NOT COALESCE(h.is_deleted, FALSE)

LEFT JOIN ${source_catalog}.gold.dim_member m
    ON h.member_id = m.member_id_bk
   AND m.is_current

LEFT JOIN ${source_catalog}.gold.dim_date d
    ON e.status_effective_date = d.calendar_date

LEFT JOIN ${source_catalog}.gold.dim_date td
    ON CAST(e.event_ts AS DATE) = td.calendar_date;
