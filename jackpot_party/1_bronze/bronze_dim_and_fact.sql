-- Inspect Table
SELECT *
FROM csv.`dbfs:/Volumes/jackpot_party/source_data/raw/jackpot_party/users.csv`
LIMIT 10;

SELECT *
FROM csv.`dbfs:/Volumes/jackpot_party/source_data/raw/jackpot_party/users.csv`
WITH (header = 'true')
LIMIT 10;

--users
CREATE TABLE jackpot_party.bronze.dim_users
USING DELTA
AS
WITH users_raw AS (
  SELECT *
  FROM csv.`dbfs:/Volumes/jackpot_party/source_data/raw/jackpot_party/users.csv`
  WITH (header = 'true')
)
SELECT
  CAST(user_id AS STRING) AS user_id,
  CAST(created_at AS DATE) AS created_at,
  TO_TIMESTAMP(first_seen_ts) AS first_seen_ts,
  TO_TIMESTAMP(last_seen_ts) AS last_seen_ts,
  NULLIF(country,'null')  AS country,
  NULLIF(platform,'null')  AS platform,
  NULLIF(language,'null')  AS language,
  NULLIF(age_bucket,'null')  AS age_bucket,
  CAST(NULLIF(is_vip,'null') AS BOOLEAN) AS is_vip,
  NULLIF(vip_tier,'null') AS vip_tier,
  CAST(NULLIF(lifetime_spend_usd,'null') AS DECIMAL(12,2)) AS lifetime_spend_usd,
  CAST(NULLIF(lifetime_ads_revenue_usd,'null') AS DECIMAL(12,2)) AS lifetime_ads_revenue_usd,
  current_timestamp() AS bronze_ingest_ts
FROM users_raw;


-- offers 
CREATE TABLE jackpot_party.bronze.dim_offers
USING DELTA
AS
WITH offers_raw AS (
  SELECT *
  FROM csv.`dbfs:/Volumes/jackpot_party/source_data/raw/jackpot_party/offers.csv`
  WITH (header = 'true')
)
SELECT
  CAST(offer_id AS STRING) AS offer_id,
  NULLIF(offer_name,'null') AS offer_name,
  CAST(NULLIF(price_usd,'null') AS DECIMAL(12,2))  AS price_usd,
  CAST(NULLIF(coins_granted,'null') AS INT) AS coins_granted,
  CAST(NULLIF(vip_only,'null') AS BOOLEAN) AS vip_only,
  current_timestamp() AS bronze_ingest_ts
FROM offers_raw;


--spins
CREATE TABLE jackpot_party.bronze.fact_spins
USING DELTA
AS
WITH users_raw AS (
  SELECT *
  FROM csv.`/Volumes/jackpot_party/source_data/raw/jackpot_party/spins.csv`
  WITH (header = 'true')
)
  CAST(spin_id AS STRING) AS spin_id,
  CAST(user_id AS STRING) AS user_id,
  CAST(machine_id AS STRING) AS machine_id,

  CAST(NULLIF(bet_amount,'null') AS DECIMAL(12,2)) AS bet_amount,
  CAST(NULLIF(win_amount,'null') AS DECIMAL(12,2)) AS win_amount,
  CAST(NULLIF(net_amount,'null') AS DECIMAL(12,2)) AS net_amount,

  CAST(NULLIF(is_bonus_spin,'null') AS BOOLEAN) AS is_bonus_spin,
  TO_TIMESTAMP(spin_ts) AS spin_ts,

  current_timestamp() AS bronze_ingest_ts
FROM users_raw;


--sessions
CREATE TABLE jackpot_party.bronze.fact_sessions
USING DELTA
AS
WITH users_raw AS (
  SELECT *
  FROM csv.`/Volumes/jackpot_party/source_data/raw/jackpot_party/sessions.csv`
  WITH (header = 'true')
)
SELECT
  CAST(session_id AS STRING) AS session_id,
  CAST(user_id AS STRING) AS user_id,

  TO_TIMESTAMP(session_start_ts) AS session_start_ts,
  TO_TIMESTAMP(session_end_ts) AS session_end_ts,

 CAST(
  CAST(NULLIF(session_length_sec,'null') AS DECIMAL(10,2))
  AS INT
) AS session_length_sec
,

  NULLIF(platform,'null') AS platform,
  NULLIF(app_version,'null')  app_version,
  CAST(NULLIF(crashed,'null') AS BOOLEAN) AS crashed,

  current_timestamp() AS bronze_ingest_ts
FROM users_raw;


-- purchases
CREATE TABLE jackpot_party.bronze.fact_purchases
USING DELTA
AS
WITH purchases_raw AS (
  SELECT *
  FROM csv.`/Volumes/jackpot_party/source_data/raw/jackpot_party/purchases.csv`
  WITH (header = 'true')
)
SELECT
  CAST(transaction_id AS STRING)                          AS transaction_id,
  CAST(user_id AS STRING)                                 AS user_id,
  CAST(offer_id AS STRING)                                AS offer_id,

  TO_TIMESTAMP(purchase_ts)                               AS purchase_ts,
  CAST(NULLIF(price_usd,'null') AS DECIMAL(12,2))          AS price_usd,
  NULLIF(status,'null')                                   AS status,

  current_timestamp()                                     AS bronze_ingest_ts
FROM purchases_raw;


-- liveops
CREATE TABLE jackpot_party.bronze.fact_liveops_events
USING DELTA
AS
WITH liveops_raw AS (
  SELECT *
  FROM csv.`/Volumes/jackpot_party/source_data/raw/jackpot_party/liveops_events.csv`
  WITH (header = 'true')
)
SELECT
  CAST(liveop_event_id AS STRING) AS liveop_event_id,
  CAST(user_id AS STRING) AS user_id,


  current_timestamp() AS bronze_ingest_ts
FROM liveops_raw;


-- installs
CREATE TABLE jackpot_party.bronze.fact_installs
USING DELTA
AS
WITH installs_raw AS (
  SELECT *
  FROM csv.`dbfs:/Volumes/jackpot_party/source_data/raw/jackpot_party/installs.csv`
  WITH (header = 'true')
)
SELECT
  CAST(install_id AS STRING) AS install_id,
  CAST(user_id AS STRING) AS user_id,
  TO_TIMESTAMP(install_ts) AS install_ts,

  NULLIF(ua_source,'null') AS ua_source,
  NULLIF(campaign_id,'null') AS campaign_id,
  NULLIF(country,'null') AS country,
  CAST(NULLIF(is_reinstall,'null') AS BOOLEAN) AS is_reinstall,

  current_timestamp()  bronze_ingest_ts
FROM installs_raw;

--- game events
CREATE TABLE jackpot_party.bronze.fact_game_events
USING DELTA
AS
WITH game_events_raw AS (
  SELECT *
  FROM csv.`dbfs:/Volumes/jackpot_party/source_data/raw/jackpot_party/game_events.csv`
  WITH (header = 'true')
)
SELECT
  CAST(event_id AS STRING) AS event_id,
  CAST(user_id AS STRING) AS user_id,
  NULLIF(event_name,'null') AS event_name,

  TO_TIMESTAMP(event_ts) AS event_ts,
  TO_TIMESTAMP(received_ts) AS received_ts,

  NULLIF(platform,'null') AS platform,
  NULLIF(event_payload,'null') AS event_payload,
  current_timestamp() AS bronze_ingest_ts
FROM game_events_raw;

-- economy transaction
CREATE TABLE jackpot_party.bronze.fact_economy_transactions
USING DELTA
AS
WITH econ_raw AS (
  SELECT *
  FROM csv.`/Volumes/jackpot_party/source_data/raw/jackpot_party/economy_transactions.csv`
  WITH (header = 'true')
)
SELECT
  CAST(econ_txn_id AS STRING) AS econ_txn_id,
  CAST(user_id AS STRING) AS user_id,
  TO_TIMESTAMP(txn_ts) AS txn_ts,
  NULLIF(txn_type,'null') AS txn_type,
  CAST(NULLIF(coins_delta,'null') AS INT) AS coins_delta,
  CAST(NULLIF(is_anomaly,'null') AS BOOLEAN) AS is_anomaly,
  current_timestamp() AS bronze_ingest_ts
FROM econ_raw;


--ads 
CREATE TABLE jackpot_party.bronze.fact_ads
USING DELTA
AS
WITH ads_raw AS (
  SELECT *
  FROM csv.`/Volumes/jackpot_party/source_data/raw/jackpot_party/ads.csv`
  WITH (header = 'true')
)
SELECT
  CAST(ad_event_id AS STRING) AS ad_event_id,
  CAST(user_id AS STRING) AS user_id,

  NULLIF(ad_type,'null') AS ad_type,
  NULLIF(placement,'null') AS placement,

  TO_TIMESTAMP(impression_ts) AS impression_ts,
  CAST(NULLIF(completed,'null') AS BOOLEAN) AS completed,
  CAST(NULLIF(revenue_usd,'null') AS DECIMAL(12,2))AS revenue_usd,

  current_timestamp() AS bronze_ingest_ts
FROM ads_raw;
