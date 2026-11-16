
CREATE OR REFRESH MATERIALIZED VIEW dim_network
COMMENT 'Historical Network Dimension'
AS
SELECT
    SHA2(CONCAT_WS('|',
        'NETWORK', network_id, TO_JSON(__START_AT)
    ), 256) AS network_key,

    network_id AS network_id_bk,
    network_code,
    network_name,
    network_type,
    network_status,

    effective_start_date,
    effective_end_date,

    __START_AT AS version_start,
    __END_AT AS version_end,
    _source_batch_id,

    (__END_AT IS NULL AND NOT is_deleted) AS is_current,
    is_deleted

FROM ${source_catalog}.silver.network;
