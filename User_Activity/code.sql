WITH RankedLogs AS (
  SELECT
    user_id,
    date,
    log,
    LAG(log) OVER (PARTITION BY user_id ORDER BY date) AS prev_log
  FROM
    users_log
),
GroupedLogs AS (
  SELECT
    user_id,
    date,
    log,
    SUM(CASE WHEN log = prev_log THEN 0 ELSE 1 END) OVER (PARTITION BY user_id ORDER BY date) AS grp
  FROM
    RankedLogs
),
AggregatedLogs AS (
  SELECT
    user_id,
    MIN(date) AS start_date,
    MAX(date) AS end_date,
    log,
    COUNT(*) AS length
  FROM
    GroupedLogs
  GROUP BY
    user_id, grp, log
)
SELECT
  user_id,
  start_date,
  end_date,
  log,
  length
FROM
  AggregatedLogs
ORDER BY
  start_date, user_id;
