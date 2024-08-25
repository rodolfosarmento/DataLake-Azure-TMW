import json
from pyspark.sql import types

def table_exists(spark, catalog, database, table):
    count = spark.sql(f"SHOW TABLES FROM {catalog}.{database}")\
            .filter(f"database = '{database}' AND tableName = '{table}'")\
            .count()

    return count == 1

def import_schema(tablename):
    with open(f"{tablename}.json", "r") as open_file:
        schema_json = json.load(open_file)

    df_schema = types.StructType.fromJson(schema_json)
    return df_schema