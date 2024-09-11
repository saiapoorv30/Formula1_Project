-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

create table if not exists f1_raw.circuits(circuitId INT,
circuitRef string,
name string,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
using csv



-- COMMAND ----------

-- MAGIC %md
-- MAGIC create constructors table
-- MAGIC - sinle line JSON
-- MAGIC - simple structure

-- COMMAND ----------

create table if not exists f1_raw.constructors(constructorId INT,
constructorRef string,
name string,
nationality string,
url STRING
)
using json


-- COMMAND ----------

select * from f1_raw_constructors;

-- COMMAND ----------

