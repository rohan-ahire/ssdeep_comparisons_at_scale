# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC BUILD_LIB=1 pip install ssdeep

# COMMAND ----------

import sys

sys.path.append('/Repos/rohan.ahire@databricks.com/ssdeep_comparison_at_scale')

# COMMAND ----------

from ssdeep_databricks.compare_ssdeep_hash import compare_ssdeep_optimized
from ssdeep_databricks.transform import get_transformed_ssdeep_hash

# COMMAND ----------

search_df = spark.createDataFrame([['-1', '3:Dblm1slM4REmKDY/JO/k8KCXRsi/eSLcKoQC0pHk9CEvltlWlVlkll6xU/lX:c1sW4y18O/4i2KoQRE8sf0+loxcX']], ['id', 'ssdeep_hash'])
search_df = get_transformed_ssdeep_hash(search_df)
display(search_df)

# COMMAND ----------

hash_df = spark.read.table("rohan.ssdeep_hash_values_transformed_v2")
display(hash_df)

# COMMAND ----------

result = compare_ssdeep_optimized(spark, hash_df, search_df)
result.count()

# COMMAND ----------

display(result)

# COMMAND ----------


