import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType


@pandas_udf('int', PandasUDFType.SCALAR)
def ssdeep_compare(hash1: pd.Series, hash2: pd.Series) -> pd.Series:
    return hash1.combine(hash2, lambda h1, h2: int(ssdeep.compare(h1, h2)))


# function to compare ssdeep hashes in an optimized way
def get_query_for_optimized_ssdeep_compare(df1, df2):

    query = f"""
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
            {df1} r1
            inner join {df2} r2 on r1.chunksize = r2.chunksize
        ) t
        where
        size(t.intersect_chunk) > 0
        or size(t.intersect_double_chunk) > 0
        and r1.ssdeep_hash != r2.ssdeep_hash
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
            {df1} r1
            inner join {df2} r2 on r1.chunksize = r2.chunksize * 2
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
            {df1} r1
            inner join {df2} r2 on r1.chunksize = r2.chunksize / 2
        ) t
        where
        size(t.intersect_chunk) > 0"""

    return query


# function to compare ssdeep hashes using all possible comparisons (brute force)
def get_query_ssdeep_all_possible_comparisons(df1, df2):

    query = f"""
        select
        r1.ssdeep_hash as r1_ssdeep_hash,
        r2.ssdeep_hash as r2_ssdeep_hash
        from {df1} r1
        cross join {df2} r2
        where
        r1.ssdeep_hash != r2.ssdeep_hash"""

    return query


def compare_ssdeep_explode_and_join(a, b, c, d):

    query = f"""
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
            inner join {c} r2 on r1.chunksize = r2.chunksize
            and r1.ngram_chunk_output_exploded = r2.ngram_chunk_output_exploded
            where r1.ssdeep_hash != r2.ssdeep_hash
            union
            select
            /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash
            from
            {b} r1
            inner join {d} r2 on r1.chunksize = r2.chunksize
            and r1.ngram_double_chunk_output_exploded = r2.ngram_double_chunk_output_exploded
            where r1.ssdeep_hash != r2.ssdeep_hash
            union
            select
            /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash
            from
            {a} r1
            inner join {c} r2 on r1.chunksize = r2.chunksize * 2
            and r1.ngram_chunk_output_exploded = r2.ngram_double_chunk_output_exploded
        ) t 
        """

    return query
