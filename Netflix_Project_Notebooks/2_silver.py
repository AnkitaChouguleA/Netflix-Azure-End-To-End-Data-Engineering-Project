# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook Lookup tables

# COMMAND ----------

# MAGIC %md
# MAGIC Parameters

# COMMAND ----------

dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder","netflix_directors")


# COMMAND ----------

# MAGIC %md
# MAGIC Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder")
var_trg_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(f"abfss://bronze@netflixadlsankita.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path",f"abfss://silver@netflixadlsankita.dfs.core.windows.net/{var_trg_folder}")\
    .save()

# COMMAND ----------

