# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS jackpot_party;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC USE CATALOG jackpot_party;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS jackpot_party.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS jackpot_party.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS jackpot_party.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM jackpot_party;