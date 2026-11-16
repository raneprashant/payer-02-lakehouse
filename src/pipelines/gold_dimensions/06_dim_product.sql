
CREATE OR REFRESH MATERIALIZED VIEW dim_product
COMMENT 'Historical Product Dimension'
AS
SELECT
    SHA2(CONCAT_WS('|',
        'PRODUCT', product_id, TO_JSON(__START_AT)
    ), 256) AS product_key,

    product_id AS product_id_bk,
    product_code,
    product_name,
    line_of_business,
    product_type,
    product_status,

    effective_start_date,
    effective_end_date,

    __START_AT AS version_start,
    __END_AT AS version_end,
    _source_batch_id,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.product;
