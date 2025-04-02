# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table('yts_catalog.bronze.raw_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ## handling null values in date_uploaded and date_uploaded_unix

# COMMAND ----------

missing_dates = df.filter(col("date_uploaded").isNull())\
    .select(
        col('id'),
        col('torrents')[0]['date_uploaded'].alias('date_uploaded'),
        col('torrents')[0]['date_uploaded_unix'].alias('date_uploaded_unix')
    )

# COMMAND ----------

df = df.alias('main').join(
    missing_dates.alias('update'),
    on='id',
    how='left'
).select(
    col('main.id').alias('movie_id'),
    col('main.imdb_code'),
    col('main.slug'),
    col('main.url'),
    col('main.title'),
    col('main.title_english'),
    col('main.title_long'),
    col('main.year'),
    col('main.rating'),
    col('main.mpa_rating'),
    col('main.runtime'),
    col('main.language'),
    col('main.state'),
    # there are some movies which dont have any genres, so i kept that as other.
    coalesce(col('main.genres'), array(lit('other'))).alias('genres'),
    col('main.description_full'),
    col('main.summary'),
    col('main.synopsis'),
    date_format(coalesce(col('main.date_uploaded'), col('update.date_uploaded')), 'yyyy-MM-dd').alias('date_uploaded'),
    coalesce(col('main.date_uploaded_unix'), col('update.date_uploaded_unix')).alias('date_uploaded_unix'),
    col('main.background_image'),
    col('main.background_image_original'),
    col('main.large_cover_image'),
    col('main.medium_cover_image'),
    col('main.small_cover_image'),
    col('main.yt_trailer_code')
)

# COMMAND ----------

df.write.format('delta')\
    .mode('append')\
    .option('path', 'abfss://silver@ytsstorageaccount.dfs.core.windows.net/cleaned_data/')\
    .saveAsTable('yts_catalog.silver.cleaned_data')
