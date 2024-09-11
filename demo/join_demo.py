# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.filter("circuit_id<70")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df._c2, circuits_df._c3, circuits_df._c4, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df._c2, circuits_df._c3, circuits_df._c4, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df._c2, circuits_df._c3, circuits_df._c4, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df._c2, circuits_df._c3, circuits_df._c4, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi Joins

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.select(circuits_df._c2, circuits_df._c3, circuits_df._c4)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") 

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") 

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count())

# COMMAND ----------

