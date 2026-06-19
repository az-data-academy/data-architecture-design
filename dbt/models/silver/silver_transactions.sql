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

-- Silver : transactions nettoyées — devise XOF (FCFA), TVA UEMOA 18%
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
        store_country,
        customer_id,
        product_id,
        product_name,
        category,
        sub_category,
        unit_price_xof,
        quantity,
        discount_pct,
        -- Recalcul total HT et TTC (TVA UEMOA 18%)
        ROUND(quantity * unit_price_xof * (1 - discount_pct), 0)        AS total_ht_xof,
        ROUND(quantity * unit_price_xof * (1 - discount_pct) * 1.18, 0) AS total_ttc_xof,
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
      AND unit_price_xof > 0
      AND quantity > 0
      AND (discount_pct IS NULL OR discount_pct <= 1.0)
)

SELECT * FROM cleaned
