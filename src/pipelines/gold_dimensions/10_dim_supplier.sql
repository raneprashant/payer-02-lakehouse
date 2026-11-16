
CREATE OR REFRESH MATERIALIZED VIEW dim_supplier
COMMENT 'Historical Supplier Organization Dimension'
AS
SELECT
    SHA2(CONCAT_WS('|',
        'SUPPLIER', supplier_id, TO_JSON(__START_AT)
    ), 256) AS supplier_key,

    supplier_id AS supplier_id_bk,
    organization_npi,
    supplier_name,
    supplier_type,
    tax_id_synthetic,
    supplier_status,

    effective_start_date,
    effective_end_date,

    __START_AT AS version_start,
    __END_AT AS version_end,
    _source_batch_id,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.supplier;
