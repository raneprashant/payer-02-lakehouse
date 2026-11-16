
CREATE OR REFRESH MATERIALIZED VIEW dim_procedure
COMMENT 'Procedure Code Dimension with reference matching'
AS
WITH used_codes AS (
    SELECT DISTINCT
        UPPER(TRIM(procedure_code)) AS procedure_code
    FROM ${source_catalog}.silver.claim_line
    WHERE procedure_code IS NOT NULL

    UNION

    SELECT DISTINCT
        UPPER(TRIM(procedure_code)) AS procedure_code
    FROM ${source_catalog}.silver.provider_contract_rate
    WHERE procedure_code IS NOT NULL
),

reference_procedures AS (
    SELECT
        UPPER(TRIM(code)) AS procedure_code,
        UPPER(TRIM(code_set)) AS code_set,
        description,
        category,
        code_status,
        effective_start_date,
        COALESCE(
            effective_end_date, DATE '2999-12-31'
        ) AS effective_end_date
    FROM ${source_catalog}.silver.reference_codes
    WHERE NOT COALESCE(is_deleted, FALSE)
      AND (
          UPPER(TRIM(code_set)) IN (
              'PROCEDURE', 'PROCEDURE_CODE',
              'CPT', 'HCPCS', 'CPT_HCPCS'
          )
          OR UPPER(TRIM(code_set)) LIKE '%PROCEDURE%'
      )
),

mapped AS (
    SELECT
        r.*,
        TRUE AS reference_found
    FROM reference_procedures r
    INNER JOIN used_codes u
        ON r.procedure_code = u.procedure_code
),

unmapped AS (
    SELECT
        u.procedure_code,
        'UNMAPPED' AS code_set,
        CAST(NULL AS STRING) AS description,
        CAST(NULL AS STRING) AS category,
        'UNMAPPED' AS code_status,
        DATE '1900-01-01' AS effective_start_date,
        DATE '2999-12-31' AS effective_end_date,
        FALSE AS reference_found
    FROM used_codes u
    LEFT ANTI JOIN reference_procedures r
        ON u.procedure_code = r.procedure_code
),

all_codes AS (
    SELECT * FROM mapped
    UNION ALL
    SELECT * FROM unmapped
)

SELECT
    SHA2(CONCAT_WS('|',
        'PROCEDURE',
        code_set,
        procedure_code,
        CAST(effective_start_date AS STRING)
    ), 256) AS procedure_key,

    procedure_code AS procedure_code_bk,
    code_set AS code_set_bk,

    description AS procedure_description,
    category AS procedure_category,
    code_status,

    effective_start_date,
    effective_end_date,
    reference_found

FROM all_codes;
