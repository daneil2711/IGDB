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
from pyspark.sql import SparkSession
from delta import *
import sys
sys.path.insert(0, '/usr/notebooks/IGDB/src/scripts')
from ingestor import IngestaoBronze
import argparse

if __name__ == "__main__":
    builder = SparkSession.builder \
        .appName("DagApp") \
        .enableHiveSupport() 
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # %%
    parser = argparse.ArgumentParser(description="Ingestão Bronze para IGDB")
    parser.add_argument('--table', type=str, required=True, help='Nome da tabela a ser ingerida')
    parser.add_argument('--id_fields', type=str, required=True, help='Campos de ID separados por vírgula')
    parser.add_argument('--timestamp_field', type=str, required=True, help='Campo de timestamp')
    args = parser.parse_args()

    # %%
    spark.sql('CREATE DATABASE IF NOT EXISTS bronze_igdb')

    # %%
    table = args.table
    path_full_load = f'/users/Daniel/data/raw/IGDB/{table}'
    path_incremental = f'/users/Daniel/data/raw/IGDB/{table}'
    file_format = 'json'
    table_name = table
    database_name = 'bronze_igdb'
    id_fields = args.id_fields.split(',')
    timestamp_field = args.timestamp_field
    partition_fields = []
    read_options = {'multiLine': 'true'}
    print(table)
    print(id_fields)
    print(timestamp_field)

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
        #!hdfs dfs -rm -R {ingestao.checkpoint_path}
    # %%
    ingestao.process_stream()