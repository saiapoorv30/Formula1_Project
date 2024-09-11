# Databricks notebook source
# MAGIC %md
# MAGIC Ingest qualifying.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", StringType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
                                        
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("/mnt/formaula1dl/raw/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("driverid", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formaula1dl/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formaula1dl/processed/qualifying"))

# COMMAND ----------

