# Databricks notebook source
from pyspark.sql.functions import explode

hash_df = (
    spark.read.table("rohan.ssdeep_hash_integer_values")
    .withColumnRenamed("chunk", "ngram_chunk_output")
    .withColumnRenamed("double_chunk", "ngram_double_chunk_output")
)

hash_df_1 = hash_df.select("ssdeep_hash", "chunksize", explode("ngram_chunk_output").alias("ngram_chunk_output_exploded"))
hash_df_2 = hash_df.select("ssdeep_hash", "chunksize", explode("ngram_double_chunk_output").alias("ngram_double_chunk_output_exploded"))

hash_df_1.write.saveAsTable("rohan.ssdeep_hash_integer_values_chunk_exploded")
hash_df_2.write.saveAsTable("rohan.ssdeep_hash_integer_values_double_chunk_exploded")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC optimize rohan.ssdeep_hash_integer_values_chunk_exploded zorder by chunksize

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC optimize rohan.ssdeep_hash_integer_values_double_chunk_exploded zorder by chunksize

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   /*+  BROADCASTJOIN(r2) */
# MAGIC   r1.ssdeep_hash as r1_ssdeep_hash,
# MAGIC   r2.ssdeep_hash as r2_ssdeep_hash
# MAGIC from
# MAGIC   rohan.ssdeep_hash_integer_values_chunk_exploded r1
# MAGIC   inner join rohan.ssdeep_hash_integer_values_chunk_exploded r2 on r1.chunksize = r2.chunksize
# MAGIC   and r1.ngram_chunk_output_exploded = r2.ngram_chunk_output_exploded
# MAGIC union
# MAGIC select
# MAGIC   /*+  BROADCASTJOIN(r2) */
# MAGIC   r1.ssdeep_hash as r1_ssdeep_hash,
# MAGIC   r2.ssdeep_hash as r2_ssdeep_hash
# MAGIC from
# MAGIC   rohan.ssdeep_hash_integer_values_double_chunk_exploded r1
# MAGIC   inner join rohan.ssdeep_hash_integer_values_double_chunk_exploded r2 on r1.chunksize = r2.chunksize
# MAGIC   and r1.ngram_double_chunk_output_exploded = r2.ngram_double_chunk_output_exploded
# MAGIC union
# MAGIC select
# MAGIC   /*+  BROADCASTJOIN(r2) */
# MAGIC   r1.ssdeep_hash as r1_ssdeep_hash,
# MAGIC   r2.ssdeep_hash as r2_ssdeep_hash
# MAGIC from
# MAGIC   rohan.ssdeep_hash_integer_values_chunk_exploded r1
# MAGIC   inner join rohan.ssdeep_hash_integer_values_double_chunk_exploded r2 on r1.chunksize = r2.chunksize * 2
# MAGIC   and r1.ngram_chunk_output_exploded = r2.ngram_double_chunk_output_exploded
# MAGIC union
# MAGIC select
# MAGIC   /*+  BROADCASTJOIN(r2) */
# MAGIC   r1.ssdeep_hash as r1_ssdeep_hash,
# MAGIC   r2.ssdeep_hash as r2_ssdeep_hash
# MAGIC from
# MAGIC   rohan.ssdeep_hash_integer_values_chunk_exploded r1
# MAGIC   inner join rohan.ssdeep_hash_integer_values_double_chunk_exploded r2 on r1.chunksize = r2.chunksize / 2
# MAGIC   and r1.ngram_chunk_output_exploded = r2.ngram_double_chunk_output_exploded

# COMMAND ----------


