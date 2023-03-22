from pyspark.ml.feature import NGram
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
import base64
from struct import unpack
import pandas as pd
from pyspark.sql.types import StringType, IntegerType, ArrayType, LongType, StructField, StructType
from pyspark.sql.functions import col, pandas_udf, PandasUDFType

# Update the schema for the UDF return type
result_schema = StructType([
    StructField("chunksize", IntegerType(), nullable=True),
    StructField("chunk", ArrayType(LongType()), nullable=True),
    StructField("double_chunk", ArrayType(LongType()), nullable=True)
])

def get_all_7_char_chunks(h):
    return set((unpack("<Q", base64.b64decode(h[i:i+7] + "=") + b"\x00\x00\x00")[0] for i in range(len(h) - 6)))

# Use the pandas_udf decorator and the updated result schema
@pandas_udf(result_schema, PandasUDFType.SCALAR)
def preprocess_hash(h: pd.Series) -> pd.DataFrame:
    def process_hash(h: str):
        block_size, h = h.split(":", 1)
        block_size = int(block_size)

        # Reduce any sequence of the same char greater than 3 to 3
        for c in set(list(h)):
            while c * 4 in h:
                h = h.replace(c * 4, c * 3)

        block_data, double_block_data = h.split(":")
        return (block_size, list(get_all_7_char_chunks(block_data)), list(get_all_7_char_chunks(double_block_data)))

    # Apply the process_hash function to each element in the Series and convert the result into a DataFrame
    result = h.apply(process_hash).apply(pd.Series)
    result.columns = ["chunksize", "chunk", "double_chunk"]
    return result


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
