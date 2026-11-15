
CREATE OR REFRESH MATERIALIZED VIEW fact_member_enrollment_change
COMMENT 'Enrollment segment transitions for tier, PCP and date boundaries'
AS
WITH ordered_segments AS (
    SELECT
        e.*,

        LAG(e.enrollment_id) OVER (
            PARTITION BY e.coverage_id
            ORDER BY e.enrollment_start_date, e.enrollment_id
        ) AS previous_enrollment_id,

        LAG(e.tier_code) OVER (
            PARTITION BY e.coverage_id
            ORDER BY e.enrollment_start_date, e.enrollment_id
        ) AS previous_tier_code,

        LAG(e.pcp_practitioner_role_id) OVER (
            PARTITION BY e.coverage_id
            ORDER BY e.enrollment_start_date, e.enrollment_id
        ) AS previous_pcp_role_id,

        LAG(e.enrollment_end_date) OVER (
            PARTITION BY e.coverage_id
            ORDER BY e.enrollment_start_date, e.enrollment_id
        ) AS previous_enrollment_end_date

    FROM ${source_catalog}.silver.enrollment e
    WHERE NOT COALESCE(e.is_deleted, FALSE)
),

transitions AS (
    SELECT
        *,
        NOT (previous_tier_code <=> tier_code)
            AS tier_changed,

        NOT (
            previous_pcp_role_id <=>
            pcp_practitioner_role_id
        ) AS pcp_changed,

        NOT (
            DATE_ADD(previous_enrollment_end_date, 1)
            <=> enrollment_start_date
        ) AS boundary_changed

    FROM ordered_segments
    WHERE previous_enrollment_id IS NOT NULL
),

actual_changes AS (
    SELECT *
    FROM transitions
    WHERE tier_changed OR pcp_changed OR boundary_changed
)

SELECT
    SHA2(
        CONCAT_WS('|', 'ENROLLMENT_CHANGE', c.enrollment_id),
        256
    ) AS enrollment_change_key,

    c.enrollment_id,
    c.previous_enrollment_id,
    c.coverage_id,
    c.member_id AS member_id_bk,

    m.member_key,

    c.previous_tier_code AS old_tier_code,
    c.tier_code AS new_tier_code,

    c.previous_pcp_role_id AS old_pcp_role_id,
    c.pcp_practitioner_role_id AS new_pcp_role_id,

    c.tier_changed,
    c.pcp_changed,
    c.boundary_changed,

    c.change_reason_code,
    c.coverage_revision,

    c.previous_enrollment_end_date,
    c.enrollment_start_date AS change_effective_date,
    c.enrollment_end_date,

    d.date_key AS change_date_key,

    'CURRENT_MEMBER_VERSION' AS member_key_resolution_method

FROM actual_changes c

LEFT JOIN ${source_catalog}.gold.dim_member m
    ON c.member_id = m.member_id_bk
   AND m.is_current

LEFT JOIN ${source_catalog}.gold.dim_date d
    ON c.enrollment_start_date = d.calendar_date;
