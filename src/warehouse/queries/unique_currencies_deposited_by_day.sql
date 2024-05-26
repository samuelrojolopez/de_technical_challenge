SELECT
    DATE_TRUNC('day'), event_timestamp) as mvnt_day
    , currency
FROM movements_model
WHERE
    TRUE
    -- AND DATE_TRUNC('day'), event_timestamp) = '2024-05-25' -- Add date if you need a specific day
    AND action = 'deposit'
GROUP BY mvnt_day, currency