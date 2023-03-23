import ssdeep
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf('int', PandasUDFType.SCALAR)
def ssdeep_compare(hash1: pd.Series, hash2: pd.Series) -> pd.Series:
    """
    Compare two ssdeep hashes using the ssdeep library.

    Parameters:
    hash1 (pandas.Series[str]): Series of ssdeep hashes.
    hash2 (pandas.Series[str]): Series of ssdeep hashes.

    Returns:
    pandas.Series[int]: Series of integer values, with the result of comparing the two hashes.
    """
    return hash1.combine(hash2, lambda h1, h2: int(ssdeep.compare(h1, h2)))


def get_query_for_optimized_ssdeep_compare(df1: str, df2: str) -> str:
    """
    Generate SQL query to compare ssdeep hashes in an optimized way.

    Both input dataframes need 3 input columns
    - ssdeep_hash
    - chunksize
    - ngram_chunk_output
    - ngram_double_chunk_output

    Parameters:
    df1 (str): The name of the first DataFrame to compare.
    df2 (str): The name of the second DataFrame to compare.

    Returns:
    str: The SQL query to compare ssdeep hashes in an optimized way.
    """

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
        and t.r1_ssdeep_hash != t.r2_ssdeep_hash
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


def get_query_ssdeep_all_possible_comparisons(df1: str, df2: str) -> str:
    """
    Generate SQL query to compare ssdeep hashes using all possible comparisons (brute force).

    Both input dataframes needs the following column:
    - ssdeep_hash

    Parameters:
    df1 (str): The name of the first DataFrame to compare.
    df2 (str): The name of the second DataFrame to compare.

    Returns:
    str: The SQL query to compare ssdeep hashes using all possible comparisons (brute force).
    """

    query = f"""
        select
        r1.ssdeep_hash as r1_ssdeep_hash,
        r2.ssdeep_hash as r2_ssdeep_hash
        from {df1} r1
        cross join {df2} r2
        where
        r1.ssdeep_hash != r2.ssdeep_hash"""

    return query


def get_query_to_compare_ssdeep_using_explode_and_join(a: str, b: str, c: str, d: str) -> str:
    """
    Generate SQL query to compare ssdeep hashes using explode and join.

    Dataframes a and c need the following columns
    - ssdeep_hash
    - chunksize
    - ngram_chunk_output_exploded

    Dataframes b and d need the following columns
    - ssdeep_hash
    - chunksize
    - ngram_double_chunk_output

    Parameters:
    a (str): The name of the dataframe containing exploded values for chunk
    b (str): The name of the dataframe containing exploded values for double chunk.
    c (str): The name of the dataframe containing exploded values for chunk values for the ssdeep hashes you want to compare.
    d (str): The name of the dataframe containing exploded values for double chunk values for the ssdeep hashes you want to compare.

    Returns:
    str: The SQL query to compare ssdeep hashes using explode and join.
    """

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
            inner join {d} r2 on r1.chunksize = r2.chunksize * 2
            and r1.ngram_chunk_output_exploded = r2.ngram_double_chunk_output_exploded
            union
            select
            /*+  BROADCASTJOIN(r2) */
            r1.ssdeep_hash as r1_ssdeep_hash,
            r2.ssdeep_hash as r2_ssdeep_hash
            from
            {b} r1
            inner join {c} r2 on r1.chunksize = r2.chunksize / 2
            and r1.ngram_double_chunk_output_exploded = r2.ngram_chunk_output_exploded
        ) t 
        """

    return query
