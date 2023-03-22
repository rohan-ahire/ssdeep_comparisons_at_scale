# Databricks notebook source
# MAGIC %sh
# MAGIC BUILD_LIB=1 pip install ssdeep

# COMMAND ----------

from ssdeep_databricks.transform import preprocess_hash
from pyspark.sql.functions import col


# Use the Pandas UDF in your PySpark code
df = spark.createDataFrame([['24:eFGStrJ9u0/6vonZdkBQAVtY+AwKZq5eNDMSCvOXpmB:is0i8kBQt+AwfSD9C2kB']], ['ssdeep_hash'])

# Use the Pandas UDF directly in the select method
result = df.select(
    col("ssdeep_hash"),
    preprocess_hash(col("ssdeep_hash")).alias("preprocessed")
)

# Split the 'preprocessed' struct column into separate columns
result = result.select(
    col("ssdeep_hash"),
    col("preprocessed.chunksize").alias("chunksize"),
    col("preprocessed.chunk").alias("chunk"),
    col("preprocessed.double_chunk").alias("double_chunk")
)

  
display(result)

# COMMAND ----------


