CREATE JOB `%s:%s_stack_clean_expired_data_daily`
ON SCHEDULE EVERY 1 DAY
STARTS '%s'
DO
DELETE FROM %s_stack
WHERE last_seen < NOW() - INTERVAL %d DAY;
