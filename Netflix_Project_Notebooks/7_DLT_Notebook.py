# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Notebook - Gold Layer

# COMMAND ----------

looktable_rules = {
    "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(name="gold_netflixdirectors")
@dlt.expect_all_or_drop(looktable_rules)
def gold_netflixdirectors_func():
    df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_directors")
    df.printSchema() #Added to verify schema
    return df

# @dlt.table(
#     name = "gold_netflixdirectors"
# )

# @dlt.expect_all_or_drop(looktable_rules)
# def myfunc():
#     df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_directors")
#     return df


# COMMAND ----------

@dlt.table(name="gold_netflixcast")
@dlt.expect_all_or_drop(looktable_rules)
def gold_netflixcast_func():
    df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_cast")
    df.printSchema() #Added to verify schema
    return df

# @dlt.table(
#     name = "gold_netflixcast"
# )

# @dlt.expect_all_or_drop(looktable_rules)
# def myfunc():
#     df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_cast")
#     return df

# COMMAND ----------

@dlt.table(name="gold_netflixcountries")
@dlt.expect_all_or_drop(looktable_rules)
def gold_netflixcountries_func():
    df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_countries")
    df.printSchema() #Added to verify schema
    return df

# @dlt.table(
#     name = "gold_netflixcountries"
# )

# @dlt.expect_all_or_drop(looktable_rules)
# def myfunc():
#     df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_countries")
#     return df

# COMMAND ----------

@dlt.table(name="gold_netflixcategory")
@dlt.expect_or_drop("rule1", "show_id is NOT NULL")
def gold_netflixcategory_func():
    df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_category")
    df.printSchema() #Added to verify schema
    return df

# @dlt.table(
#     name = "gold_netflixcategory"
# )

# @dlt.expect_or_drop("rule1","show_id is NOT NULL")
# def myfunc():
#     df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_category")
#     return df

# COMMAND ----------

@dlt.table(name="gold_stg_netflixtitles")
def gold_stg_netflixtitles_func():
    df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_titles")
    df.printSchema() #Added to verify schema
    return df

# @dlt.table

# def gold_stg_netflixtitles():
#     df = spark.readStream.format("delta").load("abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_titles")
#     return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view(name="gold_trns_netflixtitles")
def gold_trns_netflixtitles_func():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("newflag", lit(1))
    df.printSchema() #Added to verify schema
    return df

# @dlt.view

# def gold_trns_netflixtitles():
#     df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
#     df = df.withColumn("newflag",lit(1))
#     return df

# COMMAND ----------

masterdata_rules = {
    "rule1" : "newflag is NOT NULL",
    "rule2" : "show_id is NOT NULL",
}

# COMMAND ----------

@dlt.table(name="gold_netflix_titles")
@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflix_titles_func():
    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")
    df.printSchema() #Added to verify schema
    return df

# @dlt.table

# @dlt.expect_all_or_drop(masterdata_rules)
# def gold_netflix_titles():

#     df = spark.readStream.table("LIVE.gold_trns_netflixtitles")

#     return df