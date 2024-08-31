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
import argparse
from feast import Entity, FeatureView
from feast import FeatureStore
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import(
    SparkSource,
)
import sys
sys.path.insert(0, '/usr/notebooks/IGDB/src/scripts')
import dbtools
import dttools

if __name__ == "__main__":
    builder = pyspark.sql.SparkSession.builder \
        .appName("FS_igdb_dag") \
        .enableHiveSupport()

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark

    # %%
    spark.sql('CREATE DATABASE IF NOT EXISTS feature_store_igdb')

    # %%
    parser = argparse.ArgumentParser(description="Feature Store IGDB")
    parser.add_argument('--table')
    parser.add_argument('--description')
    parser.add_argument('--id_fields')
    parser.add_argument('--partition_fields')
    parser.add_argument('--date_start')
    parser.add_argument('--date_stop')
    parser.add_argument('--monthly')
    args = parser.parse_args()

    # %%
    table = args.table
    database = 'feature_store_igdb'
    fs_name = f'igdb_{table}'
    table_full_name = f'{database}.{table}'
    description = args.description
    id_fields = args.id_fields
    partition_fields = args.partition_fields

    date_start = args.date_start
    date_stop = args.date_stop
    period = args.monthly
    if period == 'True':
        period = 'monthly' 
    else:
        period = 'daily'

    dates = dttools.date_range(date_start, date_stop, period)

    query = dbtools.import_query(f'/usr/notebooks/IGDB/src/silver/feature_store/etl/{table}.sql')
    df = spark.sql(query)
    schema = dbtools.schema_feature_store(df)
    store = FeatureStore(repo_path="/usr/notebooks/feast/")

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
        company_entity = Entity(name=table, join_keys=[id_fields])
        print('Entity criada')
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
