SELECT TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
       user_pseudo_id, 
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id, 
        event_name,
        geo.country,
        device.category,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') as source,
        traffic_source.medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') as campaign,
        

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE  _TABLE_SUFFIX BETWEEN '20210101' AND '20211231' 
AND event_name in ('session_start', 'view_item_list', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')
Order by user_pseudo_id, event_timestamp;
