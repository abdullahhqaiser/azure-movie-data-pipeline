# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import yaml
import json

# COMMAND ----------

path = "abfss://configs@ytsstorageaccount.dfs.core.windows.net/config.yaml"
file_contents = dbutils.fs.head(path)
config = json.loads(file_contents)
last_date = config['last_date']

# COMMAND ----------

df_source = spark.sql(f"""
          select distinct(year) as year
          from 
          yts_catalog.silver.cleaned_data
          where 
          date_uploaded > '{last_date}' and date_uploaded <= (select max(date_uploaded) from yts_catalog.silver.cleaned_data)
          """)

# COMMAND ----------

new_year_df = spark.createDataFrame([(4000,)], ["year"])
df_source = df_source.union(new_year_df)

# COMMAND ----------

df_target = DeltaTable.forPath(spark, "abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_released_year")

# COMMAND ----------

df_new_records = df_source.join(df_target.toDF(), on="year", how="leftanti")

# COMMAND ----------

rows = spark.sql("select count(*) as language_count from yts_catalog.gold.dim_released_year").collect()[0][0]

# COMMAND ----------

df_new_records = df_new_records.withColumn("year_key", row_number().over(Window.orderBy(lit(1))) + rows)

# COMMAND ----------

df_target.alias("t").merge(df_new_records.alias("s"), "t.year = s.year").whenNotMatchedInsertAll().execute()

# COMMAND ----------


