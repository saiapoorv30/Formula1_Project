# Databricks notebook source
# MAGIC %md
# MAGIC Ingest pit_stops.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("stop", StringType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("duration", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)
                                        
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiline", True) \
.json("/mnt/formaula1dl/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverid", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formaula1dl/processed/piut_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formaula1dl/processed/piut_stops"))

# COMMAND ----------

