
CREATE OR REFRESH MATERIALIZED VIEW fact_claim_payment
COMMENT 'Gold posted payment, void and recovery transactions'
AS

-- ==========================================================
-- 1. POSTED PAYMENT TRANSACTIONS
-- Grain: remittance_transaction_id
-- Source amounts are already signed.
-- ==========================================================
WITH posted_payments AS (

    SELECT
        remittance_transaction_id,
        remittance_batch_id,
        payment_id,

        claim_id,
        line_number,

        payee_supplier_id,
        payment_date,
        posted_date,

        UPPER(TRIM(transaction_type)) AS transaction_type,
        UPPER(TRIM(payment_status)) AS payment_status,

        original_transaction_id,
        currency_code,

        CAST(payment_amount AS DECIMAL(14,2))
            AS signed_payment_amount

    FROM ${source_catalog}.silver.payment_remittance

    WHERE UPPER(TRIM(payment_status)) = 'POSTED'
),

-- ==========================================================
-- 2. CLAIM-LINE DIMENSION CONTEXT
-- fact_claim_line is in the SAME Gold Facts pipeline.
-- One claim line may have multiple payment transactions.
-- ==========================================================
payment_claim_context AS (

    SELECT
        p.*,

        f.claim_line_key,

        f.member_key,
        f.plan_key,
        f.provider_key,
        f.subscription_key,

        f.practitioner_key,
        f.supplier_key AS servicing_supplier_key,
        f.supplier_location_key,
        f.network_key,
        f.specialty_key,
        f.procedure_key,

        f.product_key,
        f.employer_group_key,
        f.employer_subgroup_key,

        f.service_date_key,

        f.gold_resolution_status
            AS claim_dimension_status,

        f.member_key_resolution_method

    FROM posted_payments p

    LEFT JOIN fact_claim_line f
        ON p.claim_id = f.claim_id
       AND p.line_number = f.line_number
),

-- ==========================================================
-- 3. PAYEE SUPPLIER LOOKUP
-- Payee supplier can differ from servicing supplier.
-- Resolve against posted date.
-- Never arbitrarily select among multiple matches.
-- ==========================================================
payee_matches AS (

    SELECT
        p.remittance_transaction_id,

        COUNT(DISTINCT s.supplier_key)
            AS payee_supplier_matches,

        CASE
            WHEN COUNT(DISTINCT s.supplier_key) = 1
                THEN MAX(s.supplier_key)
        END AS payee_supplier_key

    FROM posted_payments p

    LEFT JOIN ${source_catalog}.gold.dim_supplier s
        ON p.payee_supplier_id = s.supplier_id_bk
       AND p.posted_date BETWEEN
           s.effective_start_date AND s.effective_end_date
       AND NOT s.is_deleted

    GROUP BY
        p.remittance_transaction_id
)

-- ==========================================================
-- 4. FINAL PAYMENT FACT
-- ==========================================================
SELECT

    SHA2(
        CONCAT_WS(
            '|',
            'PAYMENT',
            p.remittance_transaction_id
        ),
        256
    ) AS payment_transaction_key,

    -- Source business identifiers
    p.remittance_transaction_id,
    p.remittance_batch_id,
    p.payment_id,
    p.claim_id,
    p.line_number,
    p.original_transaction_id,

    -- Payment details
    p.transaction_type,
    p.payment_status,
    p.currency_code,

    p.payment_date,
    p.posted_date,

    -- Signed financial amount
    p.signed_payment_amount,

    -- Claim-line foreign key
    p.claim_line_key,

    -- Inherited conformed dimension keys
    p.member_key,
    p.plan_key,
    p.provider_key,
    p.subscription_key,

    p.practitioner_key,
    p.servicing_supplier_key,
    p.supplier_location_key,
    p.network_key,
    p.specialty_key,
    p.procedure_key,

    p.product_key,
    p.employer_group_key,
    p.employer_subgroup_key,

    -- Payee organization is a separate role
    p.payee_supplier_id AS payee_supplier_id_bk,
    pm.payee_supplier_key,
    pm.payee_supplier_matches,

    -- Role-playing calendar keys
    p.service_date_key,
    pd.date_key AS payment_date_key,
    dd.date_key AS posted_date_key,

    p.claim_dimension_status,
    p.member_key_resolution_method,

    -- Payment fact resolution
    CASE
        WHEN p.claim_line_key IS NULL
            THEN 'MISSING_CLAIM_LINE'

        WHEN p.signed_payment_amount IS NULL
            THEN 'MISSING_PAYMENT_AMOUNT'

        WHEN p.posted_date IS NULL
            THEN 'MISSING_POSTED_DATE'

        WHEN p.transaction_type NOT IN
            ('ISSUE', 'VOID', 'RECOVERY')
            OR p.transaction_type IS NULL
            THEN 'INVALID_TRANSACTION_TYPE'

        WHEN pm.payee_supplier_matches > 1
            THEN 'AMBIGUOUS_PAYEE_SUPPLIER'

        WHEN p.payee_supplier_id IS NOT NULL
             AND pm.payee_supplier_matches = 0
            THEN 'MISSING_PAYEE_SUPPLIER'

        ELSE 'RESOLVED'
    END AS payment_resolution_status

FROM payment_claim_context p

LEFT JOIN payee_matches pm
    ON p.remittance_transaction_id =
       pm.remittance_transaction_id

LEFT JOIN ${source_catalog}.gold.dim_date pd
    ON p.payment_date = pd.calendar_date

LEFT JOIN ${source_catalog}.gold.dim_date dd
    ON p.posted_date = dd.calendar_date;
