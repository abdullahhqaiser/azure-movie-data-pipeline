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

df_source = spark.sql(f"""
                      select
            distinct(date(date_uploaded)) as date    
          from 
          yts_catalog.silver.cleaned_data
          where 
          date_uploaded > '{last_date}' and date_uploaded <= (select max(date_uploaded) from yts_catalog.silver.cleaned_data)
          """)

# COMMAND ----------

df_target = DeltaTable.forPath(spark, "abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_date")

# COMMAND ----------

df_new_records = df_source.join(df_target.toDF(), on="date", how="leftanti")

# COMMAND ----------

df_new_records = df_new_records.withColumn("date_key", date_format("date", "yyyyMMdd").cast("int")) \
    .withColumn("day_of_week", date_format("date", "EEEE")) \
    .withColumn("day_of_month", dayofmonth("date")) \
    .withColumn("day_of_year", dayofyear("date")) \
    .withColumn("week_of_year", weekofyear("date")) \
    .withColumn("month", month("date")) \
    .withColumn("month_name", date_format("date", "MMMM")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("year", year("date")) \
    .withColumn("is_weekend", when(dayofweek("date").isin(1, 7), True).otherwise(False)) \
    .withColumn("date_iso", date_format("date", "yyyy-MM-dd")) \
    .withColumn("date_full", date_format("date", "MMMM d, yyyy")) \
    .select(
        "date_key",
        "date",
        "date_iso",
        "date_full",
        "day_of_week",
        "day_of_month",
        "day_of_year",
        "week_of_year",
        "month",
        "month_name",
        "quarter",
        "year",
        "is_weekend",
    )

# COMMAND ----------

df_target.alias("t").merge(
    df_new_records.alias("s"),
    "s.date = t.date"
).whenNotMatchedInsertAll(
).execute()
