
CREATE OR REFRESH MATERIALIZED VIEW agg_payment_reconciliation
COMMENT 'Current plan liability reconciled with posted payment transactions'
AS
WITH liability AS (
    SELECT
        plan_key,
        benefit_plan_id AS benefit_plan_id_bk,
        COUNT(*) AS claim_lines,
        SUM(net_payer_liability) AS current_net_liability,
        SUM(net_posted_payment) AS claim_fact_posted_amount
    FROM ${source_catalog}.gold.fact_claim_line
    GROUP BY plan_key, benefit_plan_id
),
payments AS (
    SELECT
        plan_key,
        SUM(signed_payment_amount) AS posted_payments,
        COUNT(*) AS posted_transactions
    FROM ${source_catalog}.gold.fact_claim_payment
    WHERE posted_date <= CURRENT_DATE()
    GROUP BY plan_key
)
SELECT
    CURRENT_DATE() AS reporting_cutoff_date,
    l.plan_key,
    l.benefit_plan_id_bk,
    l.claim_lines,
    l.current_net_liability,

    COALESCE(p.posted_transactions, 0)
        AS posted_transactions,
    COALESCE(p.posted_payments, 0)
        AS posted_payments,

    l.current_net_liability
        - COALESCE(p.posted_payments, 0)
        AS outstanding_liability,

    l.claim_fact_posted_amount
        - COALESCE(p.posted_payments, 0)
        AS payment_reconciliation_difference,

    'CURRENT_LOADED_LIABILITY' AS liability_basis

FROM liability l
LEFT JOIN payments p
    ON l.plan_key <=> p.plan_key;
