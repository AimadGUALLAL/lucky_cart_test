CREATE OR REPLACE TABLE lucky_cart.shopper_stats AS
WITH transaction_durations AS (
  SELECT
    shopper_id,
    payload.transaction_id,
    TIMESTAMP_DIFF(
      MAX(event_timestamp),
      MIN(event_timestamp),
      SECOND
    ) AS transaction_duration_sec
  FROM `lucky_cart.raw_events`
  WHERE payload.transaction_id IS NOT NULL
  GROUP BY shopper_id, payload.transaction_id
)

SELECT
  r.shopper_id,

  -- Activité
  MIN(r.event_timestamp) AS first_activity_at,
  MAX(r.event_timestamp) AS last_activity_at,

  -- Transactions
  COUNT(DISTINCT r.payload.transaction_id) AS total_transactions,
  COUNTIF(r.event_type = 'cartValidated') AS validated_transactions,
  COUNTIF(r.event_type = 'cartCanceled') AS canceled_transactions,

  SAFE_DIVIDE(
    COUNTIF(r.event_type = 'cartValidated'),
    COUNT(DISTINCT r.payload.transaction_id)
  ) AS conversion_rate,

  -- Valeur
  SUM(
    CASE WHEN r.event_type = 'cartValidated'
         THEN r.payload.total_transaction_amount
         ELSE 0 END
  ) AS total_spent,

  AVG(
    CASE WHEN r.event_type = 'cartValidated'
         THEN r.payload.total_transaction_amount
         ELSE NULL END
  ) AS avg_basket_value,

  -- Durée transaction
  AVG(d.transaction_duration_sec) AS avg_transaction_duration_sec,

  -- Stores
  COUNT(DISTINCT r.payload.store_id) AS nb_stores_visited,
  ARRAY_AGG(DISTINCT r.payload.store_id) AS stores_visited

FROM `lucky_cart.raw_events` r
LEFT JOIN transaction_durations d
  ON r.shopper_id = d.shopper_id
 AND r.payload.transaction_id = d.transaction_id

GROUP BY r.shopper_id;
