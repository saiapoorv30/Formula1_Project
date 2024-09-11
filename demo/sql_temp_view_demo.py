# Databricks notebook source
# MAGIC %md
# MAGIC Access dataframes using sql

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019 = spark.sql(f"select * from v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC Global Temporary Views

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

spark.sql("select * \
    from global_temp.gv_race_results").show()

# COMMAND ----------

