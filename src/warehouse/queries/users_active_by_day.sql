SELECT
    DATE_TRUNC('day', event_timestamp) as mvnt_day
    , COUNT(distinct user_id)
FROM movements_model
GROUP BY DATE_TRUNC('day', event_timestamp)
HAVING COUNT(event_id) > 1