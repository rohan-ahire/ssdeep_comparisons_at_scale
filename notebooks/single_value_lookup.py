# Databricks notebook source
# MAGIC %md
# MAGIC ### Single value lookup

# COMMAND ----------

import ssdeep
from pyspark.sql.functions import col
from ssdeep_databricks.transform import preprocess_hash
from ssdeep_databricks.compare_ssdeep_hash import get_query_for_optimized_ssdeep_compare, ssdeep_compare

# COMMAND ----------

# create source table and create temp view 
source_table_name = 'hive_metastore.rohan.blackberry_ssdeep_hash_20million_transformed'
hash_df = spark.read.table(source_table_name)
hash_df.createOrReplaceTempView("hash_df")

# COMMAND ----------

# Use the Pandas UDF in your PySpark code
df = spark.createDataFrame(
    [["24:eFGStrJ9u0/6vonZdkBQAVtY+AwKZq5eNDMSCvOXpmB:is0i8kBQt+AwfSD9C2kB"]],
    ["ssdeep_hash"],
)

# Use the Pandas UDF directly in the select method
result = df.select(
    col("ssdeep_hash"), preprocess_hash(col("ssdeep_hash")).alias("preprocessed")
)

# Split the 'preprocessed' struct column into separate columns
result = result.select(
    col("ssdeep_hash"),
    col("preprocessed.chunksize").alias("chunksize"),
    col("preprocessed.chunk").alias("ngram_chunk_output"),
    col("preprocessed.double_chunk").alias("ngram_double_chunk_output"),
)

result.createOrReplaceTempView("df")

# COMMAND ----------

# get the query to do get ssdeep hash pairs that need to be compared
query = get_query_for_optimized_ssdeep_compare("hash_df", "df")
result_df = spark.sql(query)

# apply the ssdeep_compare pandas udf to get the score
result_df = result_df.withColumn('score', ssdeep_compare('r1_ssdeep_hash', 'r2_ssdeep_hash'))

# filter only comparison results where score is > 0
result_df = result_df.filter("score > 0")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing results with all possible comparisons

# COMMAND ----------

from ssdeep_databricks.compare_ssdeep_hash import get_query_ssdeep_all_possible_comparisons

# get the query to do get ssdeep hash pairs for doing a cross join to get all possible combinations
query = get_query_ssdeep_all_possible_comparisons("hash_df", "df")
result_df = spark.sql(query)

# apply the ssdeep_compare pandas udf to get the score
result_df = result_df.withColumn('score', ssdeep_compare('r1_ssdeep_hash', 'r2_ssdeep_hash'))

# filter only comparison results where score is > 0
result_df = result_df.filter("score > 0")
display(result_df)

# COMMAND ----------


