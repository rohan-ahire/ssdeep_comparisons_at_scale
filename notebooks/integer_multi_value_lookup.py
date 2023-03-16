# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC BUILD_LIB=1 pip install ssdeep

# COMMAND ----------

import sys

sys.path.append('/Repos/rohan.ahire@databricks.com/ssdeep_comparison_at_scale')

# COMMAND ----------

from ssdeep_databricks.compare_ssdeep_hash import compare_ssdeep_optimized, compare_ssdeep_optimized_v2
from ssdeep_databricks.transform import get_transformed_ssdeep_hash

# COMMAND ----------

# spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
# spark.conf.get("spark.sql.adaptive.forceOptimizeSkewedJoin")
spark.conf.set("spark.sql.adaptive.forceOptimizeSkewedJoin", False)

# COMMAND ----------

search_df = (
    spark.read.table("rohan.ssdeep_hash_integer_values")
    .withColumnRenamed("chunk", "ngram_chunk_output")
    .withColumnRenamed("double_chunk", "ngram_double_chunk_output")
    .limit(10000)
#     .filter("ssdeep_hash = '12:AsJ8/gZULV8DSKUTvu3f5LXoWVKB0/jCedebOCj2MhzKCv2J3Ya0w5BJ1EfTXds:TJ84ZgvK73hMSKBACMLN+zKCvs3Y7zTG'")
)
display(search_df)

# COMMAND ----------

hash_df = (
    spark.read.table("rohan.ssdeep_hash_integer_values")
    .withColumnRenamed("chunk", "ngram_chunk_output")
    .withColumnRenamed("double_chunk", "ngram_double_chunk_output")
)
display(hash_df)

# COMMAND ----------

result = compare_ssdeep_optimized(spark, hash_df, search_df)
result = result.cache()
result.count()

# COMMAND ----------

def calculate_binned_counts(df, score_column):
  binned_counts = df.selectExpr(f"int({score_column}/10)*10 as bin").groupBy("bin").count()
  return binned_counts

# COMMAND ----------

display(calculate_binned_counts(result, 'score'))

# COMMAND ----------

display(search_df.groupBy("chunksize").count())

# COMMAND ----------

from pyspark.sql.types import Row
import ssdeep

# function to compare ssdeep hashes using all possible comparisons (brute force)
def compare_ssdeep_all_combinations(df1, df2):
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")

    df = spark.sql(
        """
        select
        r1.ssdeep_hash as r1_ssdeep_hash,
        r2.ssdeep_hash as r2_ssdeep_hash
        from df1 r1
        cross join df2 r2
        where
        r1.ssdeep_hash != r2.ssdeep_hash
  """
    )

    df = df.rdd.map(
        lambda x: Row(
            x["r1_ssdeep_hash"],
            x["r2_ssdeep_hash"],
            ssdeep.compare(x["r1_ssdeep_hash"], x["r2_ssdeep_hash"]),
        )
    ).toDF(["r1_ssdeep_hash", "r2_ssdeep_hash", "score"])

    return df


all_compare_result_df = compare_ssdeep_all_combinations(hash_df, search_df)

display(calculate_binned_counts(all_compare_result_df, 'score'))

# COMMAND ----------

display(result)

# COMMAND ----------

display(all_compare_result_df)

# COMMAND ----------


