-- Health of the Game -- 
CREATE TABLE IF NOT EXISTS jackpot_party.gold.game_health_daily AS
WITH
-- 1) Create a date spine so every day shows up even if a metric is missing
date_spine AS (
  SELECT DISTINCT DATE(install_ts) AS dt
  FROM jackpot_party.silver.fact_installs
  UNION
  SELECT DISTINCT DATE(session_start_ts) AS dt
  FROM jackpot_party.silver.fact_sessions
  UNION
  SELECT DISTINCT DATE(purchase_ts) AS dt
  FROM jackpot_party.silver.fact_purchases
),

-- 2) Installs per day
installs_daily AS (
  SELECT
    DATE(install_ts) AS dt,
    COUNT(*) AS installs
  FROM jackpot_party.silver.fact_installs
  GROUP BY DATE(install_ts)
),

-- 3) Sessions + DAU + crashed sessions per day
sessions_daily AS (
  SELECT
    DATE(session_start_ts) AS dt,
    COUNT(*) AS sessions,
    COUNT(DISTINCT user_id) AS dau,
    SUM(CASE WHEN crashed = TRUE THEN 1 ELSE 0 END) AS crashed_sessions
  FROM jackpot_party.silver.fact_sessions
  GROUP BY DATE(session_start_ts)
),

-- 4) Revenue per day (exclude non-successful statuses)
revenue_daily AS (
  SELECT
    DATE(purchase_ts) AS dt,
    SUM(price_usd) AS revenue_usd
  FROM jackpot_party.silver.fact_purchases
  WHERE LOWER(status) = 'success'
  GROUP BY DATE(purchase_ts)
)

SELECT
  d.dt AS date,

  COALESCE(i.installs, 0) AS installs,
  COALESCE(s.dau, 0) AS dau,
  COALESCE(s.sessions, 0) AS sessions,

  COALESCE(r.revenue_usd, 0) AS revenue_usd,

  -- ARPDAU
  COALESCE(r.revenue_usd, 0) / NULLIF(COALESCE(s.dau, 0), 0) AS arpdau,

  -- crash rate (% of sessions that crashed)
  COALESCE(s.crashed_sessions, 0) / NULLIF(COALESCE(s.sessions, 0), 0) AS crashes_pct,

  -- (optional but useful for dashboards)
  COALESCE(s.crashed_sessions, 0) AS crashed_sessions

FROM date_spine d
LEFT JOIN installs_daily i ON d.dt = i.dt
LEFT JOIN sessions_daily s ON d.dt = s.dt
LEFT JOIN revenue_daily r ON d.dt = r.dt
ORDER BY d.dt;


CREATE OR REPLACE VIEW jackpot_party.gold.segment_monetization AS
WITH purch AS (
  SELECT
    user_id,
    SUM(CASE WHEN status = 'success' THEN price_usd ELSE 0 END) AS purchase_revenue_usd
  FROM jackpot_party.silver.fact_purchases
  GROUP BY user_id
),
ads_u AS (
  SELECT
    user_id,
    SUM(revenue_usd) AS ad_revenue_usd
  FROM jackpot_party.silver.fact_ads
  GROUP BY user_id
)
SELECT
  u.country,
  u.platform,
  COUNT(DISTINCT u.user_id) AS users,

  SUM(COALESCE(p.purchase_revenue_usd,0)) AS purchase_revenue_usd,
  SUM(COALESCE(a.ad_revenue_usd,0))       AS ad_revenue_usd,
  SUM(COALESCE(p.purchase_revenue_usd,0) + COALESCE(a.ad_revenue_usd,0)) AS total_revenue_usd,

  SUM(COALESCE(p.purchase_revenue_usd,0) + COALESCE(a.ad_revenue_usd,0)) / COUNT(DISTINCT u.user_id) AS arpu_usd,
  SUM(CASE WHEN COALESCE(p.purchase_revenue_usd,0) > 0 THEN 1 ELSE 0 END) / COUNT(DISTINCT u.user_id) AS payer_rate
FROM jackpot_party.silver.dim_users u
LEFT JOIN purch p ON u.user_id = p.user_id
LEFT JOIN ads_u a ON u.user_id = a.user_id
GROUP BY u.country, u.platform;

--offers
CREATE OR REPLACE VIEW jackpot_party.gold.offer_performance AS
SELECT
  o.offer_id,
  o.offer_name,
  o.price_usd AS offer_price_usd,

  COUNT(*) AS successful_purchases,
  SUM(p.price_usd) AS revenue_usd,
  COUNT(DISTINCT p.user_id) AS unique_buyers
FROM jackpot_party.silver.fact_purchases p
JOIN jackpot_party.silver.dim_offers o
  ON p.offer_id = o.offer_id
WHERE p.status = 'success'
GROUP BY o.offer_id, o.offer_name, o.price_usd;


--machine id
CREATE OR REPLACE VIEW jackpot_party.gold.machine_performance AS
SELECT
  machine_id,
  COUNT(*) AS spins,
  COUNT(DISTINCT user_id) AS unique_players,
  SUM(bet_amount) AS bet_amount_total,
  SUM(win_amount) AS win_amount_total,
  SUM(net_amount) AS net_amount_total
FROM jackpot_party.silver.fact_spins
GROUP BY machine_id;


CREATE OR REPLACE VIEW jackpot_party.gold.vip_user_rollup AS
SELECT
  u.user_id,
  u.is_vip,
  u.vip_tier,
  u.country,
  u.platform,
  COALESCE(p.purchase_revenue_usd,0) AS purchase_revenue_usd,
  COALESCE(a.ad_revenue_usd,0)       AS ad_revenue_usd,
  COALESCE(p.purchase_revenue_usd,0) + COALESCE(a.ad_revenue_usd,0) AS total_revenue_usd,
  COALESCE(s.sessions,0)             AS sessions,
  COALESCE(s.session_time_sec,0)     AS session_time_sec
FROM jackpot_party.silver.dim_users u
LEFT JOIN (
  SELECT user_id, SUM(CASE WHEN status='success' THEN price_usd ELSE 0 END) AS purchase_revenue_usd
  FROM jackpot_party.silver.fact_purchases
  GROUP BY user_id
) p ON u.user_id=p.user_id
LEFT JOIN (
  SELECT user_id, SUM(revenue_usd) AS ad_revenue_usd
  FROM jackpot_party.silver.fact_ads
  GROUP BY user_id
) a ON u.user_id=a.user_id
LEFT JOIN (
  SELECT user_id, COUNT(*) AS sessions, SUM(session_length_sec) AS session_time_sec
  FROM jackpot_party.silver.fact_sessions
  GROUP BY user_id
) s ON u.user_id=s.user_id;


-- LiveOps 

CREATE OR REPLACE VIEW jackpot_party.gold.liveops_engagement AS
WITH liveops_participants AS (
  SELECT DISTINCT
    user_id
  FROM jackpot_party.silver.fact_liveops_events
),

user_sessions AS (
  SELECT
    user_id,
    COUNT(*)                AS sessions,
    SUM(session_length_sec) AS session_time_sec,
    AVG(session_length_sec) AS avg_session_length_sec
  FROM jackpot_party.silver.fact_sessions
  GROUP BY user_id
)

SELECT
  CASE
    WHEN lp.user_id IS NOT NULL THEN 'participant'
    ELSE 'non_participant'
  END AS liveops_cohort,

  COUNT(DISTINCT u.user_id)                    AS users,
  AVG(COALESCE(us.sessions, 0))                AS avg_sessions_per_user,
  AVG(COALESCE(us.session_time_sec, 0))        AS avg_session_time_sec_per_user,
  AVG(COALESCE(us.avg_session_length_sec, 0))  AS avg_session_length_sec

FROM jackpot_party.silver.dim_users u
LEFT JOIN liveops_participants lp
  ON u.user_id = lp.user_id
LEFT JOIN user_sessions us
  ON u.user_id = us.user_id
GROUP BY 1;



