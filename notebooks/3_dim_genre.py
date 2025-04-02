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
          select movie_id, genres from 
          yts_catalog.silver.cleaned_data
          where 
          date_uploaded > '{last_date}' and date_uploaded <= (select max(date_uploaded) from yts_catalog.silver.cleaned_data)
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating dim - genre

# COMMAND ----------

new_genre = df.select(explode('genres').alias("genre_name")).distinct()

# COMMAND ----------

existing_genre = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_genre')

# COMMAND ----------

genre_to_insert = new_genre.alias("new").join(
    existing_genre.alias("existing"),
    on=col("new.genre_name") == col("existing.genre_name"),
    how = "leftanti"
).select(col('new.genre_name'))

# COMMAND ----------

rows = spark.sql("select count(*) as genre_count from yts_catalog.gold.dim_genre").collect()[0][0]

# COMMAND ----------

genre_to_insert = genre_to_insert.withColumn("genre_id", row_number().over(Window.orderBy(lit(1))) + rows).select('genre_id', 'genre_name')

# COMMAND ----------

if genre_to_insert.count() > 0:

    genre_to_insert.write.format('delta')\
        .mode('append')\
        .option("path", "abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_genre")\
        .save()

else:
    print("no new genre found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating genre junction

# COMMAND ----------

existing_genre = spark.read.format('delta').load('abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_genre')

# COMMAND ----------

genre_junction  = df.select('movie_id', explode('genres').alias("genre_name"))

genre_junction = genre_junction.join(
    existing_genre,
    on = existing_genre.genre_name == genre_junction.genre_name,
    how = "inner"
).select('movie_id', 'genre_id')

# COMMAND ----------

genre_junction.write.format('delta')\
    .mode('append')\
    .option("path", "abfss://gold@ytsstorageaccount.dfs.core.windows.net/genre_junction")\
    .save()
