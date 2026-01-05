CREATE OR REPLACE TABLE lucky_cart.daily_global_stats AS
WITH final_cart_state AS (
  SELECT
    payload.transaction_id,
    shopper_id,
    payload.store_id,
    payload.device ,
    event_type,
    event_timestamp,
    payload.products,
    payload.total_transaction_amount,

    ROW_NUMBER() OVER (
      PARTITION BY payload.transaction_id
      ORDER BY event_timestamp DESC
    ) AS rn
  FROM `lucky_cart.raw_events`
)

SELECT
  DATE(event_timestamp) AS event_date,

  -- Audience
  COUNT(DISTINCT shopper_id) AS active_shoppers,
  COUNT(DISTINCT CASE
    WHEN event_type = 'cartValidated' THEN shopper_id
  END) AS purchasing_shoppers,

  -- Carts
  COUNT(DISTINCT transaction_id) AS total_carts,
  COUNTIF(event_type = 'cartValidated') AS validated_carts,
  COUNTIF(event_type = 'cartCanceled') AS abandoned_carts,

  SAFE_DIVIDE(
    COUNTIF(event_type = 'cartValidated'),
    COUNT(DISTINCT transaction_id)
  ) AS cart_conversion_rate,

  -- Produits
  AVG((
    SELECT SUM(p.quantity)
    FROM UNNEST(products) p
  )) AS avg_products_per_cart,

  AVG((
    SELECT COUNT(DISTINCT p.product_id)
    FROM UNNEST(products) p
  )) AS avg_distinct_products_per_cart,

  -- Valeur
  AVG(total_transaction_amount) AS avg_cart_value,
  SUM(
    CASE WHEN event_type = 'cartValidated'
         THEN total_transaction_amount
         ELSE 0 END
  ) AS total_revenue,

  --  Devices (achats uniquement)
  COUNTIF(event_type = 'cartValidated' AND device = 'Mobile') AS mobile_purchases,
  COUNTIF(event_type = 'cartValidated' AND device = 'Desktop') AS desktop_purchases,
  COUNTIF(
    event_type = 'cartValidated'
    AND device NOT IN ('Mobile', 'Desktop')
  ) AS other_device_purchases

FROM final_cart_state
WHERE rn = 1
GROUP BY event_date
ORDER BY event_date;
