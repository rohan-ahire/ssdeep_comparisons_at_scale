from pyspark.ml.feature import NGram
from pyspark.sql.types import DoubleType, IntegerType
import pyspark.sql.functions as F


def get_transformed_ssdeep_hash(df):
    df = (
        df.withColumn("chunksize", F.split(df["ssdeep_hash"], ":").getItem(0))
        .withColumn("chunk", F.split(df["ssdeep_hash"], ":").getItem(1))
        .withColumn("double_chunk", F.split(df["ssdeep_hash"], ":").getItem(2))
    )

    # Apply 7-gram tokenizer to the second and third parts
    ngram_chunk = NGram(n=7, inputCol="temp_chunk", outputCol="ngram_chunk_output")
    ngram_double_chunk = NGram(
        n=7, inputCol="temp_double_chunk", outputCol="ngram_double_chunk_output"
    )

    temp = df.select(
        "*",
        F.split("chunk", "").alias("temp_chunk"),
        F.split("double_chunk", "").alias("temp_double_chunk"),
    )

    temp = ngram_chunk.transform(temp).withColumn(
        "ngram_chunk_output",
        F.transform("ngram_chunk_output", lambda x: F.regexp_replace(x, " ", "")).alias(
            "ngram_chunk_output"
        ),
    )

    result = (
        ngram_double_chunk.transform(temp)
        .withColumn(
            "ngram_double_chunk_output",
            F.transform(
                "ngram_double_chunk_output", lambda x: F.regexp_replace(x, " ", "")
            ).alias("ngram_double_chunk_output"),
        )
        .drop("temp_chunk")
        .drop("temp_double_chunk")
        .select("*")
    )

    result = result.withColumn("chunksize", F.col("chunksize").cast(IntegerType()))

    return result
