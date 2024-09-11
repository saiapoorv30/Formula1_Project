-- Databricks notebook source
-- MAGIC %md
-- MAGIC - Spark sql documentation
-- MAGIC - Create database demo
-- MAGIC - Data tab in the UI
-- MAGIC - SHOW command
-- MAGIC - DESCRIBE command
-- MAGIC - find the current table

-- COMMAND ----------

create database demo;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

desc database demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select current_database();


-- COMMAND ----------

-- MAGIC %md
-- MAGIC - create managed table using python
-- MAGIC - create managed table using sql
-- MAGIC - effect of dropping a managed table
-- MAGIC - describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables;

-- COMMAND ----------

desc extended race_results_python;

-- COMMAND ----------

select * from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

create table if not exists race_results_sql
as
select * from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - create external tables using python
-- MAGIC - create external table using sql
-- MAGIC - effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC presentation_folder_path = "dbfs:/mnt/formaula1dl/presentation"
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py;


-- COMMAND ----------

create table if not exists demo.race_results_ext_sql
(race_year	int,
race_name	string,
race_date	timestamp,
_c3	string,
driver_name	string,
driver_number	int,
driver_nationality	string,
team	string,
grid	int,
fastest_lap	int,
race_time	string,
points	float,
position	int,
created_date	timestamp
)

using delta


-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql;

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - create temp view
-- MAGIC - create global temp view
-- MAGIC - create permanent view

-- COMMAND ----------

select current_database();

-- COMMAND ----------

create or replace temp view v_race_results
as
select * from demo.race_results_python where race_year = 2018;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view gv_race_results
as
select * from demo.race_results_python where race_year = 2012;

-- COMMAND ----------

select * from global_temp.gv_race_results;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

create or replace view demo.pv_race_results
as
select * from demo.race_results_python where race_year = 2012;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select * from pv_race_results;

-- COMMAND ----------

