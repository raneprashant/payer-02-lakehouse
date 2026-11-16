
CREATE OR REFRESH MATERIALIZED VIEW fact_claim_adjustment
COMMENT 'Signed claim-line adjustment event fact'
AS
SELECT
    SHA2(CONCAT_WS('|', 'ADJUSTMENT', a.adjustment_id), 256)
        AS adjustment_key,

    a.adjustment_id,
    a.claim_id,
    a.line_number,

    f.claim_line_key,
    f.member_key,
    f.plan_key,
    f.provider_key,
    f.subscription_key,

    a.adjustment_type,
    a.adjustment_reason_code,
    a.reversal_of_adjustment_id,

    CAST(a.allowed_amount_delta AS DECIMAL(14,2))
        AS allowed_amount_delta,
    CAST(a.member_responsibility_delta AS DECIMAL(14,2))
        AS member_responsibility_delta,
    CAST(a.payer_liability_delta AS DECIMAL(14,2))
        AS payer_liability_delta,

    a.adjustment_effective_date,
    a.event_ts,

    ed.date_key AS adjustment_date_key,
    td.date_key AS event_date_key,

    CASE
        WHEN f.claim_line_key IS NULL THEN 'MISSING_CLAIM_LINE'
        WHEN a.allowed_amount_delta IS NULL
          OR a.payer_liability_delta IS NULL
          OR a.member_responsibility_delta IS NULL
            THEN 'MISSING_AMOUNT'
        ELSE 'RESOLVED'
    END AS adjustment_resolution_status

FROM ${source_catalog}.silver.claim_adjustment a

LEFT JOIN fact_claim_line f
    ON a.claim_id = f.claim_id
   AND a.line_number = f.line_number

LEFT JOIN ${source_catalog}.gold.dim_date ed
    ON a.adjustment_effective_date = ed.calendar_date

LEFT JOIN ${source_catalog}.gold.dim_date td
    ON CAST(a.event_ts AS DATE) = td.calendar_date;
