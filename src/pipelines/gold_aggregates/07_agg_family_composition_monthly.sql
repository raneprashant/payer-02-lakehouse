
CREATE OR REFRESH MATERIALIZED VIEW agg_family_composition_monthly
COMMENT 'Monthly covered family composition'
AS
WITH families AS (
    SELECT
        month_start_date,
        subscription_id_bk,
        MAX(coverage_market) AS coverage_market,
        MAX(employer_group_id_bk) AS employer_group_id_bk,

        COUNT(*) AS attached_members,

        COUNT_IF(covered_days > 0) AS family_size,

        COUNT_IF(
            covered_days > 0
            AND UPPER(relationship_code) = 'SELF'
        ) AS subscribers,

        COUNT_IF(
            covered_days > 0
            AND UPPER(relationship_code) = 'SPOUSE'
        ) AS spouses,

        COUNT_IF(
            covered_days > 0
            AND UPPER(relationship_code) IN ('CHILD', 'DEPENDENT')
        ) AS dependents,

        SUM(covered_member_month_equivalent)
            AS covered_member_months

    FROM ${source_catalog}.gold.fact_subscription_member_month

    GROUP BY
        month_start_date,
        subscription_id_bk
)
SELECT
    month_start_date AS reporting_month,
    coverage_market,
    employer_group_id_bk,
    family_size,

    COUNT(*) AS family_count,
    SUM(attached_members) AS attached_members,
    SUM(subscribers) AS subscribers,
    SUM(spouses) AS spouses,
    SUM(dependents) AS dependents,
    SUM(covered_member_months) AS member_months

FROM families
GROUP BY
    month_start_date,
    coverage_market,
    employer_group_id_bk,
    family_size;
