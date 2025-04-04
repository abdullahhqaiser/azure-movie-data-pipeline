# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import yaml
import json

# COMMAND ----------

path = "abfss://configs@ytsstorageaccount.dfs.core.windows.net/config.json"
file_contents = dbutils.fs.head(path)
config = json.loads(file_contents)
last_date = config['last_date']

# COMMAND ----------

last_date = '2025-04-02'

# COMMAND ----------

df_source = spark.sql(f"""
          select distinct(slug) as slug, url, title, description_full, summary, synopsis, background_image_original, large_cover_image
          from 
          yts_catalog.silver.cleaned_data
          where 
          date_uploaded > '{last_date}' and date_uploaded <= (select max(date_uploaded) from yts_catalog.silver.cleaned_data)
          """)

# COMMAND ----------

rows = spark.sql("select count(*) as movie_count from yts_catalog.gold.dim_movie").collect()[0][0]

# COMMAND ----------

df_source = df_source.withColumn("movie_key", row_number().over(Window.orderBy(lit(1))) + rows)\
    .select('movie_key', 'slug', 'url', 'title', 'description_full', 'summary', 'synopsis', 'background_image_original','large_cover_image')

# COMMAND ----------

df_target = DeltaTable.forPath(spark, "abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_movie")

# COMMAND ----------

df_target.alias("t").merge(df_source.alias("s"), "s.slug = t.slug").whenNotMatchedInsertAll().execute()
