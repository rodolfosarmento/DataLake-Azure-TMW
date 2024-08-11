# Databricks notebook source
# Lendo os arquivos csv (especificando schema interno e delimitador)

df_full_load = spark.read\
    .format("csv")\
    .option("header", "True")\
    .option("inferSchema", "True")\
    .option("delimiter", ";")\
    .load("/Volumes/raw/upsell/full_load/customers/*.csv")

# Exibição do dataframe

df_full_load.display()

# COMMAND ----------

# Escrevendo estes arquivo na camada bronze

df_full_load\
    .coalesce(1)\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("bronze.upsell.customers")

# COMMAND ----------

# Lendo os arquivos parquet

df_cdc = spark.read\
    .format("parquet")\
    .load("/Volumes/raw/upsell/cdc/customers/*.parquet")

# Exibição do dataframe

df_cdc.display()

# COMMAND ----------

# Criação de view temporária

df_cdc.createOrReplaceTempView("customers")

# COMMAND ----------

query = '''
select * from customers
qualify row_number() over (partition by idcustomer order by modified_date desc) = 1
'''

df_cdc_unique = spark.sql(query)
df_cdc_unique.display()


# COMMAND ----------

import delta

# COMMAND ----------

bronze  = delta.DeltaTable.forName(spark, "bronze.upsell.customers")

# COMMAND ----------

bronze.alias("b")\
    .merge(df_cdc_unique.alias("d"), "b.idCustomer = d.idCustomer")\
    .whenMatchedDelete(condition="d.OP = 'D'")\
    .whenMatchedUpdate(condition="d.OP = 'U'", set={})\
    .whenNotMatchedInsertAll(condition="d.OP = 'I' OR d.OP = 'U'")\
    .execute()
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.upsell.customers
