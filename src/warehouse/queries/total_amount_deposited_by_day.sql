SELECT
    DATE_TRUNC('day'), event_timestamp) as mvnt_day
    , currency
    , SUM(amount)                       as total_amount
FROM movements_model
WHERE
    TRUE
    -- AND DATE_TRUNC('day'), event_timestamp) = '2024-05-25' -- Add date if you need a specific day
    -- AND currency in ('usd')                                -- Specify the needed currency here
    AND action = 'deposit'
GROUP BY mvnt_day, currency