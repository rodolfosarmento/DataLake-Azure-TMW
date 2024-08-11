# Databricks notebook source
# DBTITLE 1,Importando Bibliotecas
import delta

# COMMAND ----------

# DBTITLE 1,Função de identificação de existência de tabela
def table_exists(catalog, database, table):
    count = spark.sql(f"SHOW TABLES FROM {catalog}.{database}")\
            .filter(f"database = '{database}' AND tableName = '{table}'")\
            .count()

    return count == 1

# COMMAND ----------

# DBTITLE 1,Scopo das tabelas a ser criada
catalog = "bronze"
schema = "upsell"
tablename = dbutils.widgets.get("tablename")
id_field = dbutils.widgets.get("id_field ")
timestamp_field = dbutils.widgets.get("timestamp_field")

# COMMAND ----------

# DBTITLE 1,Ingestão na camada Bronze
# Escrevendo estes arquivo na camada bronze
if not table_exists(catalog, schema, tablename):
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

# DBTITLE 1,Lendo os arquivos CDC
# Lendo os arquivos parquet

df_cdc = spark.read\
    .format("parquet")\
    .load(f"/Volumes/raw/{schema}/cdc/{tablename}/*.parquet")\
    .createOrReplaceTempView(f"view_{tablename}")

query = f'''
select * from view_{tablename}
qualify row_number() over (partition by {id_field} order by {timestamp_field} desc) = 1
'''

df_cdc_unique = spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Escrita do CDC
bronze  = delta.DeltaTable.forName(spark, f"{catalog}.{schema}.{tablename}")

# Upsert
bronze.alias("b")\
    .merge(df_cdc_unique.alias("d"), f"b.{id_field} = d.{id_field}")\
    .whenMatchedDelete(condition="d.OP = 'D'")\
    .whenMatchedUpdate(condition="d.OP = 'U'", set={})\
    .whenNotMatchedInsertAll(condition="d.OP = 'I' OR d.OP = 'U'")\
    .execute()
    
