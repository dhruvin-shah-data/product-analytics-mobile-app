-- Completed all the table checks manually before creating the silver tables
-- NULL, Dedup, %LIKE, PK Checks

-- DIM TABLES
CREATE TABLE IF NOT EXISTS jackpot_party.silver.dim_users AS
SELECT
  user_id,
  created_at,
  first_seen_ts,
  last_seen_ts,
  country,
  platform,
  language,
  age_bucket,
  is_vip,
  vip_tier,
  lifetime_spend_usd,
  lifetime_ads_revenue_usd
FROM jackpot_party.bronze.dim_users;


CREATE TABLE IF NOT EXISTS jackpot_party.silver.dim_offers AS
SELECT
  offer_id,
  offer_name,
  price_usd,
  coins_granted,
  vip_only
FROM jackpot_party.bronze.dim_offers;


-- FACT TABLES


-- Installs
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_installs AS
SELECT
  install_id,
  user_id,
  install_ts,
  ua_source,
  campaign_id,
  is_reinstall
FROM jackpot_party.bronze.fact_installs;

-- Sessions
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_sessions AS
SELECT
  session_id,
  user_id,
  session_start_ts,
  session_end_ts,
  session_length_sec,
  platform,
  app_version,
  crashed
FROM jackpot_party.bronze.fact_sessions;

-- Spins
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_spins AS
SELECT
  spin_id,
  user_id,
  machine_id,
  bet_amount,
  win_amount,
  net_amount,
  is_bonus_spin,
  spin_ts
FROM jackpot_party.bronze.fact_spins;

-- Purchases
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_purchases AS
SELECT
  transaction_id,
  user_id,
  offer_id,
  purchase_ts,
  price_usd,
  status
FROM jackpot_party.bronze.fact_purchases;

-- Ads
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_ads AS
SELECT
  ad_event_id,
  user_id,
  ad_type,
  placement,
  impression_ts,
  completed,
  revenue_usd
FROM jackpot_party.bronze.fact_ads;

-- Game Events
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_game_events AS
SELECT
  event_id,
  user_id,
  event_name,
  event_ts,
  received_ts,
  platform,
  event_payload
FROM jackpot_party.bronze.fact_game_events;

-- Economy Transactions
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_economy_transactions AS
SELECT
  econ_txn_id,
  user_id,
  txn_ts,
  txn_type,
  coins_delta,
  is_anomaly
FROM jackpot_party.bronze.fact_economy_transactions;

-- LiveOps Events
CREATE TABLE IF NOT EXISTS jackpot_party.silver.fact_liveops_events AS
SELECT
  liveops_event_id,
  user_id,
  event_type,
  join_ts,
  score,
  completed
FROM jackpot_party.bronze.fact_liveops_events;



