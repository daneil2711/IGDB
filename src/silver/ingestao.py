# %%
import pyspark
from delta import *
import sys
sys.path.insert(0, '/usr/notebooks/IGDB/src/scripts')
import dbtools
import argparse

if __name__ == "__main__":
    builder = pyspark.sql.SparkSession.builder \
        .appName("bronze_IGDB_DAG") \
        .enableHiveSupport()

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark
    # %%
    parser = argparse.ArgumentParser(description="Ingestão Silver para IGDB")
    parser.add_argument('--table', type=str, required=True, help='Nome da tabela a ser ingerida')
    args = parser.parse_args()
    # %%
    spark.sql('CREATE DATABASE IF NOT EXISTS silver_igdb')
    # %%
    table = args.table
    database = 'silver_igdb'
    table_full_name = f'{database}.{table}'
    # %%
    query = dbtools.import_query(f'/usr/notebooks/IGDB/src/silver/etl/{table}.sql')
    # %%
    (spark.sql(query)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(table_full_name))
