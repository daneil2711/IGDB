from feast import Field
from feast.types import Int64, String, Float32, UnixTimestamp
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType,LongType, DoubleType

def table_exists(spark, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1


def import_query(path, **kwargs):
    with open(path, 'r',**kwargs) as open_file:
        return open_file.read()
    

def get_schemas_from_catalog(spark, catalog, ignored=[]):
    ignored = ",".join([f"'{i}'" for i in ignored])
    databases = (spark.sql(f"SHOW DATABASES FROM {catalog}")
                      .filter(f"databaseName not in ({ignored})") 
                      .toPandas()["databaseName"]
                      .tolist())
    return databases


def get_tables_from_database(spark, database):
    tables = (spark.sql(f"SHOW TABLES FROM {database}")
                   .toPandas()['tableName']
                   .tolist())
    return tables

def feature_exists(store, table):
    try:
        store.get_feature_view(table)
        return True
    except:
        return False
    
def map_spark_type_to_feast_type(spark_type):
    if isinstance(spark_type, IntegerType):
        return Int64
    elif isinstance(spark_type, LongType):
        return Int64
    elif isinstance(spark_type, StringType):
        return String
    elif isinstance(spark_type, FloatType) or isinstance(spark_type, DoubleType):
        return Float32
    elif isinstance(spark_type, TimestampType):
        return UnixTimestamp
    else:
        raise ValueError(f"Tipo não suportado: {spark_type}")
    
def schema_feature_store(df):
    schema =[]
    for field in df.schema.fields:
        schema.append(Field(name=field.name, dtype=map_spark_type_to_feast_type(field.dataType)))
    return schema
