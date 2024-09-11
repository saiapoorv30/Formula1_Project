# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@formaula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formaula1dl.dfs.core.windows.net"))

# COMMAND ----------

