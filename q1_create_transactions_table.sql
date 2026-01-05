CREATE OR REPLACE TABLE lucky_cart.transactions AS
SELECT
  payload.transaction_id AS transaction_id,
  payload.store_id AS store_id,
  shopper_id,
  event_type AS last_event_type,
  payload.transaction_status AS last_transaction_status,
  payload.total_transaction_amount AS final_amount,
  event_timestamp AS last_event_timestamp,
  CASE 
    WHEN event_type = 'cartValidated' THEN 'Completed'
    WHEN event_type = 'cartCanceled' THEN 'Canceled'
    ELSE 'Pending'
  END as final_status
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY payload.transaction_id
      ORDER BY event_timestamp DESC
    ) AS rn
  FROM `lucky_cart.raw_events`
  WHERE payload.transaction_id IS NOT NULL
)
WHERE rn = 1;
