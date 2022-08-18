# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Separate Notebooks or One Notebook in a Pipeline?
# MAGIC 
# MAGIC 
# MAGIC Instead of defining all the tables in one single notebook, user may prefer to define tables across multiple notebooks in a pipeline for abstraction or to leverage from more than one language (ex. have one notebook in SQL, while writing the other one in Python. The order that you define your queries, or importing libraries does not matter in DLT. 
# MAGIC 
# MAGIC ##### Want to test this functionality out yourself? #######
# MAGIC To test this functionality, comment out cmd 4, cmd 10, cmd 12 and cmd 13 in previous notebook, and add this notebook to your pipeline path and refresh the pipeline.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

loanStats = spark.conf.get("loanStats")

# COMMAND ----------

# DBTITLE 1,Read Raw Data in DLT via Autoloader (Table 3: reference_loan_stats) + Optimize Data Layout for Performance
@dlt.create_table(comment="Raw historical transactions", 
                  table_properties={"quality":"bronze",
                     "delta.autoOptimize.optimizeWrite":"true",
                     "delta.tuneFileSizesForRewrites":"true",
                     "pipelines.autoOptimize.managed":"true",
                     "pipelines.autoOptimize.zOrderCols":"CustomerID, InvoiceNo",
                     "pipelines.trigger.interval":"1 hour"})
def reference_loan_stats():
  return (
    spark.readStream.format("cloudFiles")  ### TODO ###
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(f"{loanStats}"))

# COMMAND ----------

@dlt.create_table(comment="Historical loan transactions", 
                 table_properties={"quality":"silver"})
def historical_txs():
  rls = dlt.read_stream("reference_loan_stats")
  rat = dlt.read("ref_accounting_treatment") ### TO DO ###
  return (
    rls.join(rat, "id", "inner")
      .select(rls.columns)
  )

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Gold layer
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-4.png" width="500"/>
# MAGIC 
# MAGIC Our last step is to materialize the Gold Layer.
# MAGIC 
# MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

# COMMAND ----------

@dlt.create_table(
  comment="Combines historical and new loan data for unified rollup of loan balances",
  table_properties={ "quality" : "gold","pipelines.autoOptimize.zOrderCols": "location_code"})
def total_loan_balances_1():
  return (
    dlt.read("historical_txs")
      .groupBy("addr_state")
      .agg(F.sum("revol_bal").alias("bal"))
      .withColumnRenamed("addr_state", "location_code")
      .union(
        dlt.read("cleaned_new_txs")
          .groupBy("country_code")
          .agg(F.sum("balance").alias("bal"))
          .withColumnRenamed("country_code", "location_code")
      )          
  )

# COMMAND ----------

@dlt.create_table(comment="Combines historical and new loan data for unified rollup of loan balances", table_properties={ "quality" : "gold"})
def total_loan_balances_2():
  return (
    dlt.read("historical_txs")
      .groupBy("addr_state")
      .agg(F.sum("revol_bal").alias("bal"))
      .withColumnRenamed("addr_state", "location_code")
      .union(
        dlt.read("cleaned_new_txs")
          .groupBy("country_code")
          .agg(F.sum("balance").alias("bal"))
          .withColumnRenamed("country_code", "location_code")
      )
  )
