-- requete 1 pour créer la table intermediaire : 

CREATE OR REPLACE TABLE lucky_cart.cart_products AS
SELECT payload.transaction_id AS transaction_id, 
       shopper_id, 
       event_timestamp, 
       p.product_id, 
       p.quantity 
FROM `lucky_cart.raw_events`
CROSS JOIN UNNEST(payload.products) AS p 
WHERE event_type = 'cartUpdated' AND payload.transaction_id IS NOT NULL;

-- requete 2 : pour créer la table add_to_basket : 

CREATE OR REPLACE TABLE lucky_cart.add_to_basket AS
WITH intermediate_ordered_table AS (
  SELECT
    transaction_id,
    shopper_id,
    product_id,
    event_timestamp,
    quantity as current_quantity ,
    LAG(quantity) OVER (
      PARTITION BY transaction_id, product_id
      ORDER BY event_timestamp
    ) AS previous_quantity
  FROM lucky_cart.cart_products 
  )
SELECT
  transaction_id,
  shopper_id,
  product_id,
  event_timestamp,
  current_quantity ,
  COALESCE(previous_quantity, 0) as previous_quantity,
  current_quantity - COALESCE(previous_quantity, 0) AS quantity_delta,
  CASE 
    WHEN current_quantity > COALESCE(previous_quantity, 0) THEN 'added'
    WHEN current_quantity < COALESCE(previous_quantity, 0) THEN 'removed'
    ELSE 'unchanged'
  END as action_type
FROM intermediate_ordered_table
WHERE current_quantity != COALESCE(previous_quantity, 0);
  

