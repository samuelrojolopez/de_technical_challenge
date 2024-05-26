SELECT
    user_id
FROM movements_model
WHERE TRUE
    AND action = 'deposit'
    AND event_timestamp < CURRENT_DATE() -- Modify current date with your desired date to review.
GROUP BY user_id
HAVING COUNT(event_id) > 5