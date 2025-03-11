WITH user_sessions AS ( 
  SELECT 
    user_pseudo_id, 
    event_timestamp, 
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date, 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source, 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium, 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign, 
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id, 
    event_name 
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name IN ('session_start', 'add_to_cart', 'begin_checkout', 'purchase') 
),
session_metrics AS (
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS user_sessions_count,
    COUNTIF(event_name = 'add_to_cart') AS add_to_cart_count,
    COUNTIF(event_name = 'begin_checkout') AS begin_checkout_count,
    COUNTIF(event_name = 'purchase') AS purchase_count 
  FROM
    user_sessions
  GROUP BY
    event_date, source, medium, campaign
),
conversion_rates AS (
  SELECT
    event_date,
    source,
    medium,
    campaign,
    user_sessions_count,
    SAFE_DIVIDE(add_to_cart_count, user_sessions_count) AS visit_to_cart,
    SAFE_DIVIDE(begin_checkout_count, user_sessions_count) AS visit_to_checkout, 
    SAFE_DIVIDE(purchase_count, user_sessions_count) AS visit_to_purchase 
  FROM
    session_metrics
)
SELECT 
  event_date,
  source,
  medium,
  campaign,
  user_sessions_count,
  visit_to_cart,
  visit_to_checkout,
  visit_to_purchase
FROM 
  conversion_rates
ORDER BY 
  event_date, source, medium, campaign;