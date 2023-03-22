# Databricks notebook source
# MAGIC %md
# MAGIC ### Convert a single value ssdeep hash conversion to list of integers
# MAGIC 
# MAGIC Code referenced from article - https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale

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

# MAGIC %md
# MAGIC ### Converting a batch of ssdeep hash values to list of integers
# MAGIC 
# MAGIC Code referenced from article - https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale

# COMMAND ----------

# Provide table names here
source_table_name = 'hive_metastore.rohan.blackberry_ssdeep_hash_20million' # source table name should have a column called as ssdeep_hash
destination_table_name = 'hive_metastore.rohan.blackberry_ssdeep_hash_20million_transformed'

# Use the Pandas UDF in your PySpark code
df = spark.read.table(source_table_name)

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

result.write.saveAsTable(destination_table_name)

# COMMAND ----------


