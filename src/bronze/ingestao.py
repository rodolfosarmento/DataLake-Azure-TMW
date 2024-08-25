# Databricks notebook source
# DBTITLE 1,Importando Bibliotecas
import delta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import sys
sys.path.insert(0,"../lib/")
import utils

# COMMAND ----------

# DBTITLE 1,Escopo das tabelas a ser criada
catalog = "bronze"
schema = "upsell"
tablename = "transactions_product"
id_field = "idTransactionCart"
timestamp_field = "modified_date"
df_schema = utils.import_schema(tablename)
# tablename = dbutils.widgets.get("tablename")
# id_field = dbutils.widgets.get("id_field")
# timestamp_field = dbutils.widgets.get("timestamp_field")

# COMMAND ----------

# DBTITLE 1,Ingestão na camada Bronze
# Escrevendo estes arquivo na camada bronze
if not utils.table_exists(spark, catalog, schema, tablename):
    print("Tabela não existente, criando...")

    # Lendo os arquivos csv
    df_full_load = spark.read\
                    .format("csv")\
                    .option("header", "True")\
                    .option("inferSchema", "True")\
                    .option("delimiter", ";")\
                    .load(f"/Volumes/raw/{schema}/full_load/{tablename}/*.csv")
    
    # Escrevendo em delta
    df_full_load\
        .coalesce(1)\
        .write\
        .format("delta")\
        .mode("overwrite")\
        .saveAsTable(f"{catalog}.{schema}.{tablename}")

else:
    print("Tabela já existente, ignorando full-load")

# COMMAND ----------

# Lendo os arquivos csv
df_full_load = spark.read\
                .format("csv")\
                .option("header", "True")\
                .option("inferSchema", "True")\
                .option("delimiter", ";")\
                .load(f"/Volumes/raw/{schema}/full_load/{tablename}/*.csv")

# COMMAND ----------

df_cdc = spark.read\
    .format("parquet")\
    .load(f"/Volumes/raw/{schema}/cdc/{tablename}/*.parquet")

# COMMAND ----------

# DBTITLE 1,Lendo os arquivos CDC
# Tabela Bronze
bronze  = delta.DeltaTable.forName(spark, f"{catalog}.{schema}.{tablename}")

# Função upsert
def upsert(df, deltatable):
    
    df.createOrReplaceGlobalTempView(f"view_{tablename}")

    query = f'''
    select * from global_temp.view_{tablename}
    qualify row_number() over (partition by {id_field} order by {timestamp_field} desc) = 1
    '''

    df_cdc_unique = spark.sql(query)

    deltatable.alias("b")\
        .merge(df_cdc_unique.alias("d"), f"b.{id_field} = d.{id_field}")\
        .whenMatchedDelete(condition="d.OP = 'D'")\
        .whenMatchedUpdate(condition="d.OP = 'U'", set={})\
        .whenNotMatchedInsertAll(condition="d.OP = 'I' OR d.OP = 'U'")\
        .execute()

# Definido a Stream
df_stream = spark.readStream\
            .format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .schema(df_schema)\
            .load(f"/Volumes/raw/{schema}/cdc/{tablename}/*.parquet") 

# Persistências dos Dados
stream = df_stream.writeStream\
        .option("checkpointLocation",f"/Volumes/raw/{schema}/cdc/{tablename}_checkpoint/")\
        .foreachBatch(lambda df, batchID: upsert(df,bronze))\
        .trigger(availableNow=True)

# COMMAND ----------

stream.start()
