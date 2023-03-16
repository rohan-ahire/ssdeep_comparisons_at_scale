# Databricks notebook source
import base64
from struct import unpack

def get_all_7_char_chunks(h):
    return set((unpack("<Q", base64.b64decode(h[i:i+7] + "=") + b"\x00\x00\x00")[0] for i in range(len(h) - 6)))


def preprocess_hash(h):
    block_size, h = h.split(":", 1)
    block_size = int(block_size)

    # Reduce any sequence of the same char greater than 3 to 3
    for c in set(list(h)):
        while c * 4 in h:
            h = h.replace(c * 4, c * 3)

    block_data, double_block_data = h.split(":")
    return (block_size, list(get_all_7_char_chunks(block_data)), list(get_all_7_char_chunks(double_block_data)))

# COMMAND ----------

from pyspark.sql.types import Row
from pyspark.sql.functions import col

df = spark.read.table("rohan.ssdeep_hash_values_20m").select(
    "ssdeep_hash"
)

result = df.rdd.map(
    lambda x: Row(x["ssdeep_hash"], preprocess_hash(x["ssdeep_hash"]))
).toDF(["ssdeep_hash", "preprocessed"])
result = result.select(
    col("ssdeep_hash"),
    col("preprocessed._1").alias("chunksize"),
    col("preprocessed._2").alias("chunk"),
    col("preprocessed._3").alias("double_chunk")
)

result.write.mode("overwrite").saveAsTable("rohan.ssdeep_hash_integer_values")

# COMMAND ----------

display(spark.read.table("rohan.ssdeep_hash_integer_values").count())

# COMMAND ----------

display(spark.read.table("rohan.ssdeep_hash_integer_values").limit(10))
