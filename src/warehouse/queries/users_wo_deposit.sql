SELECT
    user_id
FROM user_id as u
LEFT JOIN movements_model as m ON u.user_id = m.user_id
WHERE TRUE
    AND action = 'deposit'
GROUP BY user_id
HAVING COUNT(event_id) < 1
