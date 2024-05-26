SELECT
    user_id
    , last_login
FROM user_login_events_model
WHERE TRUE
    -- AND user_id in ('') -- Add the user_id filter if you need specific user_ids to filter
