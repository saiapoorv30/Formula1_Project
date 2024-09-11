-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_raw;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

