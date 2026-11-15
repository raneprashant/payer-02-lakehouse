
CREATE OR REFRESH MATERIALIZED VIEW fact_subscription_member_month
COMMENT 'Monthly family subscription attachment and covered days'
AS
WITH current_relationships AS (
    SELECT
        subscription_member_id,
        subscription_id,
        member_id,
        relationship_code,
        relationship_start_date,

        LEAST(
            COALESCE(relationship_end_date, DATE '2999-12-31'),
            CURRENT_DATE()
        ) AS reporting_end_date

    FROM ${source_catalog}.silver.subscription_membership
    WHERE __END_AT IS NULL
      AND NOT COALESCE(is_deleted, FALSE)
),

valid_relationships AS (
    SELECT *
    FROM current_relationships
    WHERE relationship_start_date IS NOT NULL
      AND relationship_start_date <= reporting_end_date
),

expanded AS (
    SELECT
        r.*,
        EXPLODE(
            SEQUENCE(
                TRUNC(r.relationship_start_date, 'MM'),
                TRUNC(r.reporting_end_date, 'MM'),
                INTERVAL 1 MONTH
            )
        ) AS month_start_date

    FROM valid_relationships r
),

relationship_slices AS (
    SELECT
        e.*,

        GREATEST(
            e.relationship_start_date,
            e.month_start_date
        ) AS attached_start_date,

        LEAST(
            e.reporting_end_date,
            LAST_DAY(e.month_start_date)
        ) AS attached_end_date

    FROM expanded e
),

-- Distinct membership dates per subscription/member/month
attached_months AS (
    SELECT
        subscription_id,
        member_id,
        month_start_date,

        ARRAY_DISTINCT(
            FLATTEN(
                COLLECT_LIST(
                    SEQUENCE(
                        attached_start_date,
                        attached_end_date,
                        INTERVAL 1 DAY
                    )
                )
            )
        ) AS attached_dates,

        MAX(attached_end_date) AS last_attached_date,

        COUNT(DISTINCT subscription_member_id)
            AS membership_record_count,

        COUNT(DISTINCT relationship_code)
            AS relationship_type_count,

        CASE
            WHEN COUNT(DISTINCT relationship_code) = 1
                THEN MAX(relationship_code)
        END AS relationship_code

    FROM relationship_slices
    GROUP BY
        subscription_id, member_id, month_start_date
),

-- Combine distinct days from all Coverage rows in that month
covered_months AS (
    SELECT
        subscription_id_bk AS subscription_id,
        member_id_bk AS member_id,
        month_start_date,

        ARRAY_DISTINCT(
            FLATTEN(
                COLLECT_LIST(
                    SEQUENCE(
                        covered_start_date,
                        covered_end_date,
                        INTERVAL 1 DAY
                    )
                )
            )
        ) AS covered_dates

    FROM fact_member_coverage_month

    GROUP BY
        subscription_id_bk,
        member_id_bk,
        month_start_date
),

dimension_matches AS (
    SELECT
        a.subscription_id,
        a.member_id,
        a.month_start_date,

        COUNT(DISTINCT s.subscription_key)
            AS subscription_matches,

        COUNT(DISTINCT m.member_key)
            AS member_matches,

        CASE
            WHEN COUNT(DISTINCT s.subscription_key) = 1
                THEN MAX(s.subscription_key)
        END AS subscription_key,

        CASE
            WHEN COUNT(DISTINCT m.member_key) = 1
                THEN MAX(m.member_key)
        END AS member_key,

        CASE
            WHEN COUNT(DISTINCT s.subscription_key) = 1
                THEN MAX(s.coverage_market)
        END AS coverage_market,

        CASE
            WHEN COUNT(DISTINCT s.subscription_key) = 1
                THEN MAX(s.subscriber_member_id_bk)
        END AS subscriber_member_id_bk,

        CASE
            WHEN COUNT(DISTINCT s.subscription_key) = 1
                THEN MAX(s.employer_group_id_bk)
        END AS employer_group_id_bk,

        CASE
            WHEN COUNT(DISTINCT s.subscription_key) = 1
                THEN MAX(s.employer_subgroup_id_bk)
        END AS employer_subgroup_id_bk

    FROM attached_months a

    LEFT JOIN ${source_catalog}.gold.dim_subscription s
        ON a.subscription_id = s.subscription_id_bk
       AND a.last_attached_date BETWEEN
           s.effective_start_date AND s.effective_end_date
       AND NOT s.is_deleted

    LEFT JOIN ${source_catalog}.gold.dim_member m
        ON a.member_id = m.member_id_bk
       AND m.is_current

    GROUP BY
        a.subscription_id,
        a.member_id,
        a.month_start_date
),

monthly_measures AS (
    SELECT
        a.*,

        SIZE(a.attached_dates) AS attached_days,

        SIZE(
            ARRAY_INTERSECT(
                a.attached_dates,
                COALESCE(
                    c.covered_dates,
                    CAST(ARRAY() AS ARRAY<DATE>)
                )
            )
        ) AS covered_days

    FROM attached_months a

    LEFT JOIN covered_months c
        ON a.subscription_id = c.subscription_id
       AND a.member_id = c.member_id
       AND a.month_start_date = c.month_start_date
)

SELECT
    SHA2(
        CONCAT_WS(
            '|', 'SUBSCRIPTION_MEMBER_MONTH',
            a.subscription_id,
            a.member_id,
            CAST(a.month_start_date AS STRING)
        ), 256
    ) AS subscription_member_month_key,

    a.subscription_id AS subscription_id_bk,
    a.member_id AS member_id_bk,

    k.subscription_key,
    k.member_key,

    k.subscriber_member_id_bk,
    k.employer_group_id_bk,
    k.employer_subgroup_id_bk,
    k.coverage_market,

    a.relationship_code,
    a.relationship_type_count,
    a.membership_record_count,

    a.month_start_date,
    d.date_key AS month_key,

    a.attached_days,
    a.covered_days,

    DAY(LAST_DAY(a.month_start_date)) AS days_in_month,

    CAST(
        ROUND(
            a.attached_days /
            DAY(LAST_DAY(a.month_start_date)), 6
        ) AS DECIMAL(12,6)
    ) AS attached_member_month_equivalent,

    CAST(
        ROUND(
            a.covered_days /
            DAY(LAST_DAY(a.month_start_date)), 6
        ) AS DECIMAL(12,6)
    ) AS covered_member_month_equivalent,

    k.member_matches,
    k.subscription_matches,

    CASE
        WHEN k.member_matches > 1
          OR k.subscription_matches > 1
            THEN 'AMBIGUOUS_DIMENSION'
        WHEN k.member_matches = 0
          OR k.subscription_matches = 0
            THEN 'MISSING_DIMENSION'
        WHEN a.relationship_type_count > 1
            THEN 'MULTIPLE_RELATIONSHIP_TYPES'
        ELSE 'RESOLVED'
    END AS subscription_month_resolution_status,

    'CURRENT_ACCEPTED_MEMBERSHIP_HISTORY'
        AS relationship_history_basis

FROM monthly_measures a

LEFT JOIN dimension_matches k
    ON a.subscription_id = k.subscription_id
   AND a.member_id = k.member_id
   AND a.month_start_date = k.month_start_date

LEFT JOIN ${source_catalog}.gold.dim_date d
    ON a.month_start_date = d.calendar_date;
