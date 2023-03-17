import ssdeep
from pyspark.sql.types import Row


# function to compare ssdeep hashes in an optimized way
def compare_ssdeep_optimized(spark, df1, df2):

    # set broadcast join threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1000000000")

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")

    df = spark.sql(
        """
        select
        t.r1_ssdeep_hash,
        t.r2_ssdeep_hash
        from
        (
            select /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash,
            array_intersect(r1.ngram_chunk_output, r2.ngram_chunk_output) as intersect_chunk,
            array_intersect(
                r1.ngram_double_chunk_output,
                r2.ngram_double_chunk_output
            ) as intersect_double_chunk
            from
            df1 r1
            inner join df2 r2 on r1.chunksize = r2.chunksize
        ) t
        where
        size(t.intersect_chunk) > 0
        or size(t.intersect_double_chunk) > 0
        union
        select
        t.r1_ssdeep_hash,
        t.r2_ssdeep_hash
        from
        (
            select /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash,
            array_intersect(
                r1.ngram_chunk_output,
                r2.ngram_double_chunk_output
            ) as intersect_chunk
            from
            df1 r1
            inner join df2 r2 on r1.chunksize = r2.chunksize * 2
        ) t
        where
        size(t.intersect_chunk) > 0
        union
        select
        r1_ssdeep_hash,
        r2_ssdeep_hash
        from
        (
            select /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash,
            array_intersect(
                r1.ngram_double_chunk_output,
                r2.ngram_chunk_output
            ) as intersect_chunk
            from
            df1 r1
            inner join df2 r2 on r1.chunksize = r2.chunksize / 2
        ) t
        where
        size(t.intersect_chunk) > 0
        """
    )

    df = df.rdd.map(
        lambda x: Row(
            x["r1_ssdeep_hash"],
            x["r2_ssdeep_hash"],
            int(ssdeep.compare(x["r1_ssdeep_hash"], x["r2_ssdeep_hash"])),
        )
    ).toDF(["r1_ssdeep_hash", "r2_ssdeep_hash", "score"])

    return df

# function to compare ssdeep hashes in an optimized way
def compare_ssdeep_optimized_v2(spark, df1, df2):

    # set broadcast join threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1000000000")

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")

    df = spark.sql(
        """
        select
        t.r1_ssdeep_hash,
        t.r2_ssdeep_hash
        from
        (
            select /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash,
            array_intersect(r1.ngram_chunk_output, r2.ngram_chunk_output) as intersect_chunk,
            array_intersect(
                r1.ngram_double_chunk_output,
                r2.ngram_double_chunk_output
            ) as intersect_double_chunk,
            array_intersect(
                r1.ngram_chunk_output,
                r2.ngram_double_chunk_output
            ) as intersect_chunk_double_chunk,
            array_intersect(
                r1.ngram_double_chunk_output,
                r2.ngram_chunk_output
            ) as intersect_double_chunk_chunk
            from
            df1 r1
            inner join df2 r2 on (r1.chunksize = r2.chunksize) or (r1.chunksize = r2.chunksize * 2) or (r1.chunksize = r2.chunksize / 2)
        ) t
        where
        size(t.intersect_chunk) > 0
        or size(t.intersect_double_chunk) > 0
        or size(t.intersect_chunk_double_chunk) > 0
        or size(t.intersect_double_chunk_chunk) > 0
        """
    )

    df = df.rdd.map(
        lambda x: Row(
            x["r1_ssdeep_hash"],
            x["r2_ssdeep_hash"],
            int(ssdeep.compare(x["r1_ssdeep_hash"], x["r2_ssdeep_hash"])),
        )
    ).toDF(["r1_ssdeep_hash", "r2_ssdeep_hash", "score"])

    return df


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

def calculate_binned_counts(df, score_column):
  binned_counts = df.selectExpr(f"int({score_column}/10)*10 as bin").groupBy("bin").count()
  return binned_counts


def compare_ssdeep_explode_and_join(spark, a, b, df1, df2):
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")

    result_df = spark.sql(f"""
        select
        t.r1_ssdeep_hash,
        t.r2_ssdeep_hash
        from
        (
            select
            /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash
            from
            {a} r1
            inner join df1 r2 on r1.chunksize = r2.chunksize
            and r1.ngram_chunk_output_exploded = r2.ngram_chunk_output_exploded
            union
            select
            /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash
            from
            {b} r1
            inner join df2 r2 on r1.chunksize = r2.chunksize
            and r1.ngram_double_chunk_output_exploded = r2.ngram_double_chunk_output_exploded
            union
            select
            /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash
            from
            {a} r1
            inner join df2 r2 on r1.chunksize = r2.chunksize * 2
            and r1.ngram_chunk_output_exploded = r2.ngram_double_chunk_output_exploded
        ) t 
        """)

    result_df = result_df.rdd.map(
            lambda x: Row(
                x["r1_ssdeep_hash"],
                x["r2_ssdeep_hash"],
                int(ssdeep.compare(x["r1_ssdeep_hash"], x["r2_ssdeep_hash"])),
            )
        ).toDF(["r1_ssdeep_hash", "r2_ssdeep_hash", "score"])    
    
    return result_df
