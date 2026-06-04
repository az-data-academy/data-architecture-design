{{
  config(
    materialized='table',
    file_format='iceberg',
    table_properties={
      'format-version': '2',
      'write.merge.mode': 'copy-on-write'
    }
  )
}}

-- Gold : CA hebdomadaire par magasin
-- Use case : dashboard directeurs régionaux

SELECT
    year(transaction_ts)                               AS year,
    week_of_year(transaction_ts)                      AS week_number,
    store_id,
    store_name,
    store_region,
    COUNT(transaction_id)                             AS nb_transactions,
    COUNT(DISTINCT customer_id)                       AS nb_clients_uniques,
    ROUND(SUM(total_amount) / 1.20, 2)               AS ca_total_ht,
    ROUND(SUM(total_amount), 2)                      AS ca_total_ttc,
    CURRENT_TIMESTAMP                                 AS gold_ts

FROM {{ ref('silver_transactions') }}
WHERE return_flag = FALSE

GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, ca_total_ttc DESC
