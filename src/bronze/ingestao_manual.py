# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import pyspark
from delta import *
import sys
sys.path.insert(0, '/usr/notebooks/IGDB/src/scripts')
from ingestor import IngestaoBronze

builder = pyspark.sql.SparkSession.builder.appName("Bronze") \
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
spark.sql('CREATE DATABASE IF NOT EXISTS bronze_igdb')

# %%
table = 'collections'
path_full_load = f'/users/Daniel/data/raw/IGDB/{table}'
path_incremental = f'/users/Daniel/data/raw/IGDB/{table}'
file_format = 'json'
table_name = table
database_name = 'bronze_igdb'
id_fields = ['id']
timestamp_field = 'updated_at'
partition_fields = []
read_options = {'multiLine': 'true'}

# %%
ingestao = IngestaoBronze(
    path_full_load=path_full_load,
    path_incremental=path_incremental,
    file_format=file_format,
    table_name=table_name,
    database_name=database_name,
    id_fields=id_fields,
    timestamp_field=timestamp_field,
    partition_fields=partition_fields,
    read_options=read_options,
    spark=spark)

# %%
if not spark.catalog.tableExists(f"{database_name}.{table_name}"):
    df_null = spark.createDataFrame(data=[], schema=ingestao.schema)
    ingestao.save_full(df_null)
    # # !hdfs dfs -rm -R {ingestao.checkpoint_path}

# %%
ingestao.process_stream()

# %% [markdown]
# #### Checando Resultados

# %%
spark.sql('''
show tables from bronze_igdb

''').show(truncate=False)

# %%
spark.sql('''
select count(*), count(id), count(distinct id) from
bronze_igdb.games             
''').show()

# %%
df = spark.read.json('/users/Daniel/data/raw/IGDB/characters')
df.count()

# %% [markdown]
# #### Save Schema

# %%
df = spark.sql('''
show tables from bronze_igdb

''').toPandas()

# %%
for table in df['tableName']:
    path_full_load = f'/users/Daniel/data/raw/IGDB/{table}'
    path_incremental = f'/users/Daniel/data/raw/IGDB/{table}'
    file_format = 'json'
    table_name = table
    database_name = 'bronze_igdb'
    id_fields = ['id']
    timestamp_field = 'updated_at'
    partition_fields = []
    read_options = {'multiLine': 'true'}

    ingestao = IngestaoBronze(
        path_full_load=path_full_load,
        path_incremental=path_incremental,
        file_format=file_format,
        table_name=table_name,
        database_name=database_name,
        id_fields=id_fields,
        timestamp_field=timestamp_field,
        partition_fields=partition_fields,
        read_options=read_options,
        spark=spark)
    ingestao.save_schema()

# %% [markdown]
# #### Limpando ambiente

# %%
spark.sql('drop database bronze_igdb cascade')

# %%
# !hdfs dfs -rm -R /users/Daniel/data/raw/IGDB/

# %%
spark.sql('drop database silver_igdb cascade')
