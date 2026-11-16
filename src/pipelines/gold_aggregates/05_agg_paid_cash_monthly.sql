
CREATE OR REFRESH MATERIALIZED VIEW agg_paid_cash_monthly
COMMENT 'Net posted cash including signed voids and recoveries'
AS
SELECT
    TRUNC(posted_date, 'MM') AS posted_month,
    plan_key,
    payee_supplier_key,
    currency_code,

    COUNT(*) AS transaction_count,

    COUNT_IF(transaction_type = 'ISSUE')
        AS issue_count,
    COUNT_IF(transaction_type = 'VOID')
        AS void_count,
    COUNT_IF(transaction_type = 'RECOVERY')
        AS recovery_count,

    SUM(CASE WHEN transaction_type = 'ISSUE'
        THEN signed_payment_amount ELSE 0 END)
        AS issue_amount,

    SUM(CASE WHEN transaction_type = 'VOID'
        THEN signed_payment_amount ELSE 0 END)
        AS void_amount,

    SUM(CASE WHEN transaction_type = 'RECOVERY'
        THEN signed_payment_amount ELSE 0 END)
        AS recovery_amount,

    SUM(signed_payment_amount) AS net_posted_cash

FROM ${source_catalog}.gold.fact_claim_payment
WHERE posted_date IS NOT NULL
GROUP BY
    TRUNC(posted_date, 'MM'),
    plan_key,
    payee_supplier_key,
    currency_code;
