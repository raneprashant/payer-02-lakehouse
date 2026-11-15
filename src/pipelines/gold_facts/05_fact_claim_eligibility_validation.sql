
CREATE OR REFRESH MATERIALIZED VIEW fact_claim_eligibility_validation
COMMENT 'Latest available claim-line eligibility evaluation'
AS
WITH eligibility_context AS (
    SELECT
        coverage_id,
        enrollment_id,
        COUNT(*) AS matching_segments,
        MAX(coverage_valid_flag) AS coverage_valid_flag,
        MAX(offering_valid_flag) AS offering_valid_flag,
        MAX(tier_code) AS tier_code,
        MAX(pcp_practitioner_role_id) AS pcp_practitioner_role_id
    FROM ${source_catalog}.silver.member_eligibility
    GROUP BY coverage_id, enrollment_id
)
SELECT
    SHA2(
        CONCAT_WS(
            '|', 'CLAIM_ELIGIBILITY',
            f.claim_id, CAST(f.line_number AS STRING)
        ), 256
    ) AS claim_eligibility_key,

    f.claim_id,
    f.line_number,
    f.claim_line_key,

    f.member_id AS member_id_bk,
    f.member_key,
    f.plan_key,
    f.subscription_key,

    e.coverage_id,
    e.enrollment_id,
    e.benefit_plan_id,
    e.subscription_id,

    f.service_date,
    f.service_date_key,

    e.eligibility_valid_flag,
    ec.coverage_valid_flag,
    ec.offering_valid_flag,

    ec.tier_code,
    ec.pcp_practitioner_role_id,

    en.coverage_revision AS source_coverage_revision,
    ec.matching_segments,

    e.resolution_status AS silver_claim_resolution_status,

    CASE
        WHEN e.claim_id IS NULL
            THEN 'NO_EVALUATION'
        WHEN e.coverage_id IS NULL
            THEN 'NO_COVERAGE_MATCH'
        WHEN e.enrollment_id IS NULL
            THEN 'NO_ENROLLMENT_MATCH'
        WHEN ec.matching_segments IS NULL
            THEN 'MISSING_ENROLLMENT_CONTEXT'
        WHEN ec.matching_segments > 1
            THEN 'AMBIGUOUS_ENROLLMENT_CONTEXT'
        WHEN e.eligibility_valid_flag = FALSE
          OR ec.coverage_valid_flag = FALSE
            THEN 'INVALID_ELIGIBILITY'
        WHEN ec.offering_valid_flag = FALSE
            THEN 'INVALID_OFFERING'
        WHEN e.eligibility_valid_flag = TRUE
         AND ec.coverage_valid_flag = TRUE
         AND ec.offering_valid_flag = TRUE
            THEN 'VALID'
        WHEN e.eligibility_valid_flag = TRUE
         AND ec.coverage_valid_flag = TRUE
         AND ec.offering_valid_flag IS NULL
            THEN 'OFFERING_NOT_VERIFIED'
        ELSE 'NOT_VERIFIED'
    END AS eligibility_evaluation_status

FROM fact_claim_line f

LEFT JOIN ${source_catalog}.silver.claim_enriched e
    ON f.claim_id = e.claim_id
   AND f.line_number = e.line_number

LEFT JOIN eligibility_context ec
    ON e.coverage_id = ec.coverage_id
   AND e.enrollment_id = ec.enrollment_id

LEFT JOIN ${source_catalog}.silver.enrollment en
    ON e.enrollment_id = en.enrollment_id
   AND NOT COALESCE(en.is_deleted, FALSE);
