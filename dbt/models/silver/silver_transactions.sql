{{
  config(
    materialized='table',
    file_format='iceberg',
    table_properties={
      'format-version': '2',
      'write.merge.mode': 'merge-on-read'
    }
  )
}}

-- Silver : transactions nettoyées
-- Source : couche Bronze (tables Iceberg brutes)
-- Règles appliquées :
--   1. Filtre les prix négatifs et quantités nulles
--   2. Normalise payment_method (UNKNOWN → NULL)
--   3. Corrige les incohérences return_reason / return_flag
--   4. Recalcule total_amount

WITH bronze AS (
    SELECT * FROM {{ source('bronze', 'transactions') }}
),

deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY transaction_id
               ORDER BY ingestion_ts DESC
           ) AS rn
    FROM bronze
),

cleaned AS (
    SELECT
        transaction_id,
        store_id,
        store_name,
        store_region,
        customer_id,
        product_id,
        product_name,
        category,
        sub_category,
        unit_price,
        quantity,
        discount_pct,
        -- Recalcul du montant total (règle métier documentée)
        ROUND(quantity * unit_price * (1 - discount_pct) * 1.20, 2) AS total_amount,
        CASE
            WHEN payment_method = 'UNKNOWN' THEN NULL
            ELSE payment_method
        END AS payment_method,
        channel,
        return_flag,
        CASE
            WHEN return_flag = FALSE THEN NULL
            ELSE return_reason
        END AS return_reason,
        CAST(transaction_ts AS TIMESTAMP) AS transaction_ts,
        ingestion_ts,
        CURRENT_TIMESTAMP AS silver_ts,
        '1.0' AS data_version

    FROM deduplicated
    WHERE rn = 1
      AND unit_price > 0
      AND quantity > 0
      AND (discount_pct IS NULL OR discount_pct <= 1.0)
)

SELECT * FROM cleaned
