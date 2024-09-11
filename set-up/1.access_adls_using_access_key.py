# Databricks notebook source
# MAGIC %md
# MAGIC ### Access azure data lake using access keys
# MAGIC - set the spark config fs.azure.account.key
# MAGIC - list files from demo container
# MAGIC - read data from circuits.csv.file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------


spark.conf.set("fs.azure.account.key.formaula1dl.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formaula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formaula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

