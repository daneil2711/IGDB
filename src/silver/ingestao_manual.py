# %%
import pyspark
from delta import *
import sys
sys.path.insert(0, '/usr/notebooks/IGDB/src/scripts')
import dbtools

builder = pyspark.sql.SparkSession.builder.appName("Silver") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.cores.max", "4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport()

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark
# %%
spark.sql('CREATE DATABASE IF NOT EXISTS silver_igdb')
# %%
table = 'games'
database = 'silver_igdb'
table_full_name = f'{database}.{table}'
# %%
query = dbtools.import_query(f'etl/{table}.sql')
# %%
(spark.sql(query)
      .write
      .format('delta')
      .mode('overwrite')
      .option('overwriteSchema', 'true')
      .saveAsTable(table_full_name))
# %% [markdown]
# ### Checando results

# %%
spark.sql('''
show tables from silver_igdb

''').show(truncate=False)

# %%
spark.sql('''
select * from
silver_igdb.franchises              
''').show(10)
