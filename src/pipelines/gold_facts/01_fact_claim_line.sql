
CREATE OR REFRESH MATERIALIZED VIEW fact_claim_line
COMMENT 'Gold Claim Line Fact - one current record per claim line'
AS

-- ==========================================================
-- 1. BASE CLAIM DATA
-- Grain: claim_id + line_number
-- ==========================================================
WITH base AS (

    SELECT
        l.claim_id,
        l.line_number,

        COALESCE(e.member_id, h.member_id) AS member_id,
        e.coverage_id,
        e.enrollment_id,
        e.benefit_plan_id,
        e.subscription_id,

        l.rendering_practitioner_role_id,
        e.practitioner_id,
        e.supplier_location_id,
        e.supplier_id,
        e.specialty_id,
        e.network_id,

        l.service_date,
        h.received_date,
        h.adjudication_date,

        h.claim_type,
        h.source_claim_status,
        l.adjudication_status,
        l.denial_reason_code,

        l.procedure_code,
        l.modifier_code,
        l.units,

        l.billed_amount,
        l.member_responsibility_amount,

        f.base_allowed_amount,
        f.base_payer_liability,

        f.allowed_amount_adjustment_total,
        f.payer_liability_adjustment_total,

        f.net_allowed_amount,
        f.net_payer_liability,
        f.net_posted_payment,
        f.outstanding_amount,

        f.derived_payment_status,
        f.reconciliation_status,

        (f.claim_id IS NOT NULL) AS financial_record_found,

        e.eligibility_valid_flag,
        e.resolution_status,

        e.contract_id,
        e.contract_rate_id,
        e.negotiated_rate

    FROM ${source_catalog}.silver.claim_line l

    LEFT JOIN ${source_catalog}.silver.claim_header h
        ON l.claim_id = h.claim_id
       AND NOT COALESCE(h.is_deleted, FALSE)

    LEFT JOIN ${source_catalog}.silver.claim_enriched e
        ON l.claim_id = e.claim_id
       AND l.line_number = e.line_number

    LEFT JOIN ${source_catalog}.silver.claim_financial f
        ON l.claim_id = f.claim_id
       AND l.line_number = f.line_number

    WHERE NOT COALESCE(l.is_deleted, FALSE)
),

-- ==========================================================
-- 2. DIRECT DIMENSION MATCHING
--
-- COUNT(DISTINCT key) measures ambiguity.
-- A surrogate key is assigned ONLY when exactly one
-- dimension version qualifies.
--
-- SCD2 business dates are not CDC sequence timestamps.
-- ==========================================================
direct_matches AS (

    SELECT
        b.claim_id,
        b.line_number,

        COUNT(DISTINCT m.member_key) AS member_matches,
        COUNT(DISTINCT p.plan_key) AS plan_matches,
        COUNT(DISTINCT v.provider_key) AS provider_matches,
        COUNT(DISTINCT s.subscription_key) AS subscription_matches,

        COUNT(DISTINCT pr.practitioner_key) AS practitioner_matches,
        COUNT(DISTINCT su.supplier_key) AS supplier_matches,
        COUNT(DISTINCT sl.supplier_location_key) AS location_matches,

        COUNT(DISTINCT n.network_key) AS network_matches,
        COUNT(DISTINCT sp.specialty_key) AS specialty_matches,
        COUNT(DISTINCT pc.procedure_key) AS procedure_matches,

        CASE WHEN COUNT(DISTINCT m.member_key) = 1
             THEN MAX(m.member_key) END AS member_key,

        CASE WHEN COUNT(DISTINCT p.plan_key) = 1
             THEN MAX(p.plan_key) END AS plan_key,

        CASE WHEN COUNT(DISTINCT v.provider_key) = 1
             THEN MAX(v.provider_key) END AS provider_key,

        CASE WHEN COUNT(DISTINCT s.subscription_key) = 1
             THEN MAX(s.subscription_key) END AS subscription_key,

        CASE WHEN COUNT(DISTINCT pr.practitioner_key) = 1
             THEN MAX(pr.practitioner_key) END AS practitioner_key,

        CASE WHEN COUNT(DISTINCT su.supplier_key) = 1
             THEN MAX(su.supplier_key) END AS supplier_key,

        CASE WHEN COUNT(DISTINCT sl.supplier_location_key) = 1
             THEN MAX(sl.supplier_location_key) END AS supplier_location_key,

        CASE WHEN COUNT(DISTINCT n.network_key) = 1
             THEN MAX(n.network_key) END AS network_key,

        CASE WHEN COUNT(DISTINCT sp.specialty_key) = 1
             THEN MAX(sp.specialty_key) END AS specialty_key,

        CASE WHEN COUNT(DISTINCT pc.procedure_key) = 1
             THEN MAX(pc.procedure_key) END AS procedure_key

    FROM base b

    -- Member does not have business-effective version dates.
    -- Current member profile only; not historical as-of.
    LEFT JOIN ${source_catalog}.gold.dim_member m
        ON b.member_id = m.member_id_bk
       AND m.is_current

    -- Plan business-effective at claim service date
    LEFT JOIN ${source_catalog}.gold.dim_plan p
        ON b.benefit_plan_id = p.benefit_plan_id_bk
       AND b.service_date BETWEEN
           p.effective_start_date AND p.effective_end_date
       AND NOT p.is_deleted

    -- Rendering provider role
    LEFT JOIN ${source_catalog}.gold.dim_provider v
        ON b.rendering_practitioner_role_id =
           v.practitioner_role_id_bk
       AND b.service_date BETWEEN
           v.effective_start_date AND v.effective_end_date
       AND NOT v.is_deleted
       AND v.is_current

    -- Subscription
    LEFT JOIN ${source_catalog}.gold.dim_subscription s
        ON b.subscription_id = s.subscription_id_bk
       AND b.service_date BETWEEN
           s.effective_start_date AND s.effective_end_date
       AND NOT s.is_deleted

    -- Practitioner
    LEFT JOIN ${source_catalog}.gold.dim_practitioner pr
        ON b.practitioner_id = pr.practitioner_id_bk
       AND b.service_date BETWEEN
           pr.effective_start_date AND pr.effective_end_date
       AND NOT pr.is_deleted

    -- Supplier organization
    LEFT JOIN ${source_catalog}.gold.dim_supplier su
        ON b.supplier_id = su.supplier_id_bk
       AND b.service_date BETWEEN
           su.effective_start_date AND su.effective_end_date
       AND NOT su.is_deleted

    -- Supplier location
    LEFT JOIN ${source_catalog}.gold.dim_supplier_location sl
        ON b.supplier_location_id = sl.supplier_location_id_bk
       AND b.service_date BETWEEN
           sl.effective_start_date AND sl.effective_end_date
       AND NOT sl.is_deleted

    -- Network
    LEFT JOIN ${source_catalog}.gold.dim_network n
        ON b.network_id = n.network_id_bk
       AND b.service_date BETWEEN
           n.effective_start_date AND n.effective_end_date
       AND NOT n.is_deleted

    -- Specialty (SCD1)
    LEFT JOIN ${source_catalog}.gold.dim_specialty sp
        ON b.specialty_id = sp.specialty_id_bk
       AND sp.is_current

    -- Procedure, including its effective date
    LEFT JOIN ${source_catalog}.gold.dim_procedure pc
        ON UPPER(TRIM(b.procedure_code)) =
           pc.procedure_code_bk
       AND b.service_date BETWEEN
           pc.effective_start_date AND pc.effective_end_date

    GROUP BY
        b.claim_id,
        b.line_number
),

-- ==========================================================
-- 3. RESOLVED DIRECT DIMENSION KEYS
-- ==========================================================
resolved AS (

    SELECT
        b.*,
        d.* EXCEPT (claim_id, line_number)

    FROM base b

    LEFT JOIN direct_matches d
        ON b.claim_id = d.claim_id
       AND b.line_number = d.line_number
),

-- ==========================================================
-- 4. DEPENDENT DIMENSION KEYS
-- Product comes from the resolved Plan.
-- Employer Group/Subgroup come from Subscription.
-- ==========================================================
dependent_matches AS (

    SELECT
        r.claim_id,
        r.line_number,

        COUNT(DISTINCT prod.product_key) AS product_matches,
        COUNT(DISTINCT eg.employer_group_key) AS employer_group_matches,
        COUNT(DISTINCT es.employer_subgroup_key) AS employer_subgroup_matches,

        CASE WHEN COUNT(DISTINCT prod.product_key) = 1
             THEN MAX(prod.product_key) END AS product_key,

        CASE WHEN COUNT(DISTINCT eg.employer_group_key) = 1
             THEN MAX(eg.employer_group_key) END AS employer_group_key,

        CASE WHEN COUNT(DISTINCT es.employer_subgroup_key) = 1
             THEN MAX(es.employer_subgroup_key) END AS employer_subgroup_key

    FROM resolved r

    LEFT JOIN ${source_catalog}.gold.dim_plan p
        ON r.plan_key = p.plan_key

    LEFT JOIN ${source_catalog}.gold.dim_product prod
        ON p.product_id_bk = prod.product_id_bk
       AND r.service_date BETWEEN
           prod.effective_start_date AND prod.effective_end_date
       AND NOT prod.is_deleted

    LEFT JOIN ${source_catalog}.gold.dim_subscription s
        ON r.subscription_key = s.subscription_key

    LEFT JOIN ${source_catalog}.gold.dim_employer_group eg
        ON s.employer_group_id_bk = eg.employer_group_id_bk
       AND r.service_date BETWEEN
           eg.effective_start_date AND eg.effective_end_date
       AND NOT eg.is_deleted

    LEFT JOIN ${source_catalog}.gold.dim_employer_subgroup es
        ON s.employer_subgroup_id_bk = es.employer_subgroup_id_bk
       AND r.service_date BETWEEN
           es.effective_start_date AND es.effective_end_date
       AND NOT es.is_deleted

    GROUP BY
        r.claim_id,
        r.line_number
)

-- ==========================================================
-- 5. FINAL GOLD FACT
-- ==========================================================
SELECT

    SHA2(
        CONCAT_WS(
            '|',
            'CLAIM_LINE',
            r.claim_id,
            CAST(r.line_number AS STRING)
        ),
        256
    ) AS claim_line_key,

    r.*,

    dep.* EXCEPT (claim_id, line_number),

    sd.date_key AS service_date_key,
    rd.date_key AS received_date_key,
    ad.date_key AS adjudication_date_key,

    -- Member history cannot be mapped to service date
    -- from CDC sequence alone.
    CASE
        WHEN r.member_key IS NOT NULL
            THEN 'CURRENT_MEMBER_VERSION'
        ELSE 'UNRESOLVED'
    END AS member_key_resolution_method,

    -- Do not silently select an ambiguous dimension.
    CASE
        WHEN NOT r.financial_record_found
            THEN 'MISSING_FINANCIAL'

        WHEN
            r.member_matches > 1
            OR r.plan_matches > 1
            OR r.provider_matches > 1
            OR r.subscription_matches > 1
            OR r.practitioner_matches > 1
            OR r.supplier_matches > 1
            OR r.location_matches > 1
            OR r.network_matches > 1
            OR r.specialty_matches > 1
            OR r.procedure_matches > 1
            OR dep.product_matches > 1
            OR dep.employer_group_matches > 1
            OR dep.employer_subgroup_matches > 1
            THEN 'AMBIGUOUS_DIMENSION'

        WHEN r.member_key IS NULL
            OR (r.benefit_plan_id IS NOT NULL AND r.plan_key IS NULL)
            OR (r.rendering_practitioner_role_id IS NOT NULL
                AND r.provider_key IS NULL)
            OR (r.subscription_id IS NOT NULL
                AND r.subscription_key IS NULL)
            OR (r.procedure_code IS NOT NULL
                AND r.procedure_key IS NULL)
            THEN 'MISSING_DIMENSION'

        ELSE 'RESOLVED'
    END AS gold_resolution_status

FROM resolved r

LEFT JOIN dependent_matches dep
    ON r.claim_id = dep.claim_id
   AND r.line_number = dep.line_number

LEFT JOIN ${source_catalog}.gold.dim_date sd
    ON r.service_date = sd.calendar_date

LEFT JOIN ${source_catalog}.gold.dim_date rd
    ON r.received_date = rd.calendar_date

LEFT JOIN ${source_catalog}.gold.dim_date ad
    ON r.adjudication_date = ad.calendar_date;
