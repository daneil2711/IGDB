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
import pyspark.sql.functions as F
import pandas as pd
from datetime import datetime
from feast import Entity, FeatureView
from feast import FeatureStore
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import(
    SparkSource,
)
import sys
sys.path.insert(0, '../../scripts')
# sys.path.insert(0, '/usr/notebooks/IGDB/src/scripts')
import dbtools
import dttools

builder = pyspark.sql.SparkSession.builder.appName("Feat_manual") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .enableHiveSupport()

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark

# %%
spark.sql('CREATE DATABASE IF NOT EXISTS feature_store_igdb')

# %%
table = 'company_themes'
database = 'feature_store_igdb'
fs_name = f'igdb_{table}'
table_full_name = f'{database}.{table}'
description = 'Estatísticas dos generos da compania'
id_fields = ['idCompany']
partition_fields = 'dtRef'

date_start = '2021-12-01'
date_stop = '2022-02-01'
period = 'False'
if period == 'True':
    period = 'monthly' 
else:
    period = 'daily'

dates = dttools.date_range(date_start, date_stop, period)


query = dbtools.import_query(f'etl/{table}.sql')
df = spark.sql(query)
schema = dbtools.schema_feature_store(df)
store = FeatureStore(repo_path="/usr/notebooks/feast/")

# %%
spark.sql('show tables from feature_store_igdb').show()

# %%
spark.sql(f'select * from {table_full_name} where dtref = "2022-01-07"').show(10)

# %% [markdown]
# ### Criando a Feature - similiar ao create_table do bricks

# %%
fl_table_exists = dbtools.table_exists(spark, database, table)
if not fl_table_exists:
    (spark.sql(query.format(date=dates[0]))
        .write
        .format('delta')
        .mode('overwrite')
        .partitionBy(partition_fields)
        .option('overwriteSchema', 'true')
        .saveAsTable(f'{table_full_name}'))

# %%
for i in dates:
    df = spark.sql(query.format(date=i))
    print(i)
    (df.write
        .format('delta')
        .mode('overwrite')
        .option("replaceWhere", f"{partition_fields} = '{i}'")
        .option('overwriteSchema', 'true')
        .saveAsTable(table_full_name))


# %%
fl_fs_exists = dbtools.feature_exists(store, fs_name)

if not fl_fs_exists:
    company_entity = Entity(name=table, join_keys=id_fields)
    
    source = SparkSource(
        table=f'{database}.{table}',
        name=description,
        timestamp_field=partition_fields)

    company_feature_view = FeatureView(
        name=fs_name,
        entities=[company_entity],
        ttl=None,
        schema=schema,
        online=False,
        source=source,
    )
    store.apply([company_feature_view])

# %% [markdown]
# ### Validações

# %%
feature_view = store.get_feature_view(fs_name)
entity_columns = set([entity.name for entity in feature_view.entity_columns])
feature_names = [
    f"{feature_view.name}:{field.name}"
    for field in feature_view.schema
    if field.name not in entity_columns
]

# %%
import pandas as pd

# Defina os IDs das entidades
id_company = [1]

# Cria um intervalo de datas para o período desejado
date_range = pd.date_range(start="2021-12-01", end="2022-12-01", freq='M')
# date_range = ['2021-12-01','2021-12-31','2022-01-01','2022-02-01','2022-03-01','1999-01-01','2025-01-01']

# Cria uma lista de dicionários para replicar cada ID para cada data no intervalo
data = [{"idCompany": id_, "event_timestamp": date} for id_ in id_company for date in date_range]

# Converte para DataFrame
entity_df = pd.DataFrame(data)

# %%
features = store.get_historical_features(
    features=feature_names,
    entity_df=entity_df
).to_df()

features


# %% [markdown]
# ### Logar no MLFlow

# %%
import mlflow

mlflow.set_tracking_uri("http://mlflow:5000")

# %%
feature_view = store.get_feature_view("company_general_features")
feature_names = [f"{feature_view.name}:{field.name}" for field in feature_view.schema]

feature_df = pd.DataFrame({"features": feature_names})
feature_df.to_csv("/usr/notebooks/IGDB/src/silver/feat.csv", index=False)

# %%
mlflow.set_experiment("log_feat")
with mlflow.start_run(run_name="Log de Features"):
    # Logue o arquivo de features
    mlflow.log_artifact("/usr/notebooks/IGDB/src/silver/feat.csv", artifact_path="features")

    # Registre o número de features como um parâmetro
    mlflow.log_param("num_features", len(feature_names))

    # Registre os nomes das features
    mlflow.log_param("features", ", ".join(feature_names))

