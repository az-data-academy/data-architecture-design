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

-- Gold : CA hebdomadaire par magasin — en XOF (FCFA)
SELECT
    year(transaction_ts)                                   AS year,
    week_of_year(transaction_ts)                          AS week_number,
    store_id,
    store_name,
    store_country,
    COUNT(transaction_id)                                 AS nb_transactions,
    COUNT(DISTINCT customer_id)                           AS nb_clients_uniques,
    ROUND(SUM(total_ttc_xof) / 1.18, 0)                  AS ca_total_ht_xof,
    ROUND(SUM(total_ttc_xof), 0)                          AS ca_total_ttc_xof,
    CURRENT_TIMESTAMP                                     AS gold_ts

FROM {{ ref('silver_transactions') }}
WHERE return_flag = FALSE

GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, ca_total_ttc_xof DESC
