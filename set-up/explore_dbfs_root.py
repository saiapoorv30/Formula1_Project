# Databricks notebook source
# MAGIC %md
# MAGIC ##explore dbfs root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv("/FileStore/circuits.csv"))

# COMMAND ----------

