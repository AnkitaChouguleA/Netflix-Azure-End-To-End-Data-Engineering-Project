# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Data Transformation

# COMMAND ----------

df = spark.read.format("delta")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@netflixadlsankita.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("duration_minutes", df["duration_minutes"].cast(IntegerType()))\
     .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("Shorttitle",split(col("title"), ":")[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("rating",split(col("rating"), "-")[0])
df.display()

# COMMAND ----------

df = df.withColumn("type_flag",when(col("type") == "Movie", 1)\
        .when(col("type") == "TV Show", 2)\
        .otherwise(0))
df.display()

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

df = df.withColumn("duration_ranking",
    dense_rank().over(Window.orderBy(desc("duration_minutes")))
)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

spark.sql("select * from temp_view order by duration_minutes desc").display()

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_temp_view")

# COMMAND ----------

spark.sql("select * from global_temp.global_temp_view order by duration_minutes desc").display()

# COMMAND ----------

#how many movies and tv shows we have
df_vis = df.groupBy("type").agg(count("*").alias("total_count"))
df_vis.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@netflixadlsankita.dfs.core.windows.net/netflix_titles")\
    .save()

# COMMAND ----------

