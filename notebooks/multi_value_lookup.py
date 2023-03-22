# Databricks notebook source
# MAGIC %md
# MAGIC ### Multi value lookup

# COMMAND ----------

import ssdeep
from pyspark.sql.functions import col
from ssdeep_databricks.transform import preprocess_hash
from ssdeep_databricks.compare_ssdeep_hash import get_query_for_optimized_ssdeep_compare, ssdeep_compare

# COMMAND ----------

# create source table and create temp view 
source_table_name = 'hive_metastore.rohan.blackberry_ssdeep_hash_20million_transformed'
hash_df = spark.read.table(source_table_name).withColumnRenamed("chunk", "ngram_chunk_output").withColumnRenamed("double_chunk", "ngram_double_chunk_output")
hash_df.createOrReplaceTempView("hash_df")

# COMMAND ----------

# Read ssdeep hashes you wish to compare with your corpus created in previous cell
source_table_name = 'hive_metastore.rohan.blackberry_ssdeep_hash_20million_transformed'

# I have limited to 10 records, but you can given any number of records
# you can replace this with your table containing n number of records to find similar ssdeep hashes 
search_df = spark.read.table(source_table_name).withColumnRenamed("chunk", "ngram_chunk_output").withColumnRenamed("double_chunk", "ngram_double_chunk_output").limit(100)
search_df.createOrReplaceTempView("search_df")

search_df.createOrReplaceTempView("search_df")

# COMMAND ----------

# get the query to do get ssdeep hash pairs that need to be compared
query = get_query_for_optimized_ssdeep_compare("hash_df", "search_df")
result_df = spark.sql(query)

# apply the ssdeep_compare pandas udf to get the score
result_df = result_df.withColumn('score', ssdeep_compare('r1_ssdeep_hash', 'r2_ssdeep_hash'))

# filter only comparison results where score is > 0
result_df = result_df.filter("score > 0")
display(result_df)
