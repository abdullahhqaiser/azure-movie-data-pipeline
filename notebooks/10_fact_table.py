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

df = spark.sql(f"""
          select * from 
          yts_catalog.silver.cleaned_data
          where 
          date_uploaded > '{last_date}' and date_uploaded <= (select max(date_uploaded) from yts_catalog.silver.cleaned_data)
          """)

# COMMAND ----------

dim_movie = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_movie')
dim_date = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_date')
dim_language = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_language')
dim_runtime = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_runtime')
dim_rating = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_rating')
dim_released_year = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_released_year')

# COMMAND ----------

df = df.join(dim_movie, df['slug'] == dim_movie['slug'], 'left')\
  .join(dim_date, df['date_uploaded'] == dim_date['date'], 'left')\
  .join(dim_language, df['language'] == dim_language['language'], 'left')\
  .join(dim_runtime, df['runtime'] == dim_runtime['runtime'], 'left')\
  .join(dim_rating, df['rating'] == dim_rating['rating'], 'left')\
  .join(dim_released_year, df['year'] == dim_released_year['year'], 'left')\
  .select(
      df['movie_id'], df['imdb_code'], dim_movie['movie_key'], dim_date['date_key'], dim_language['language_key'], dim_runtime['runtime_key'], dim_rating['rating_key'], dim_released_year['year_key']
  )
  

# COMMAND ----------

df_target = DeltaTable.forPath(spark, "abfss://gold@ytsstorageaccount.dfs.core.windows.net/fact_movies")

# COMMAND ----------

df_target.alias("t").merge(df.alias("s"), "s.movie_id = t.movie_id").whenNotMatchedInsertAll().execute()

# COMMAND ----------

config['last_date'] = df.select(max(df['date_uploaded'])).collect()[0][0]
config_str = json.dumps(config, indent=4)
dbutils.fs.put(config_path, config_str, overwrite=True)
