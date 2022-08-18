-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify ETL with Delta Live Table
-- MAGIC 
-- MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
-- MAGIC 
-- MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-1.png" width="700"/>
-- MAGIC 
-- MAGIC **Accelerate ETL development** <br/>
-- MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC 
-- MAGIC **Remove operational complexity** <br/>
-- MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC 
-- MAGIC **Trust your data** <br/>
-- MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC 
-- MAGIC **Simplify batch and streaming** <br/>
-- MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC 
-- MAGIC ## Our Delta Live Table pipeline
-- MAGIC 
-- MAGIC We'll be using as input a raw generated dataset containing information on our customers Loan and historical transactions. 
-- MAGIC 
-- MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC 
-- MAGIC ## Bronze layer: incrementally ingest data leveraging Databricks Autoloader
-- MAGIC 
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-2.png" width="500"/>
-- MAGIC 
-- MAGIC Our raw data is being sent to a blob storage. 
-- MAGIC 
-- MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
-- MAGIC 
-- MAGIC Autoloader is available in SQL using the `cloud_files` function and can be used with a variety of format (json, csv, avro...):
-- MAGIC 
-- MAGIC 
-- MAGIC #### STREAMING LIVE TABLE 
-- MAGIC Defining tables as `STREAMING` will guarantee that you only consume new incoming data. 
-- MAGIC - Compute results over append-only streams such as Kafka, Kinesis, or Autoloader (files on cloud storage)
-- MAGIC 
-- MAGIC - Allows you to reduce costs & latency by avoiding reprocessing of old data
-- MAGIC 
-- MAGIC - Delta tables with delta.appendOnly=true
-- MAGIC 
-- MAGIC - Produce results on demand
-- MAGIC     * Lower latency: more frequent less data processing
-- MAGIC     * Lower costs: by avoiding redundant reprocessing

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data in DLT via Autoloader (Table 1: raw_txs) with Parametrization
CREATE STREAMING LIVE TABLE raw_txs     -- TO DO --
COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files('${input_data}/landing', 'json', map("cloudFiles.schemaEvolutionMode", "rescue")) -- input_data: /home/firstname.lastname@databricks.com

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data in DLT via Autoloader (Table 3: reference_loan_stats) + Optimize Data Layout for Performance
CREATE STREAMING LIVE TABLE reference_loan_stats
COMMENT "Raw historical transactions"
TBLPROPERTIES --Can be spark, delta, or DLT confs
("quality"="bronze",
"pipelines.autoOptimize.managed"="true",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo",
"pipelines.trigger.interval"="1 hour"
 )
AS SELECT * FROM cloud_files('${loanStats}', 'csv', map("cloudFiles.inferColumnTypes", "true"))  -- loanStats: /databricks-datasets/lending-club-loan-stats/LoanStats_*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### LIVE TABLES 
-- MAGIC 
-- MAGIC - It scan and ingest all the data available at once. 
-- MAGIC - Always "correct", meaning their contents will match their definition after any update.
-- MAGIC 
-- MAGIC - Today they are computed completely, but we are going to get much more clever as time goes on (go/enzyme) . 
-- MAGIC 
-- MAGIC - Does not make sense to change them from the outside (you'll either get undefined answers or your change will just be undone)

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data Stored in Delta Tables in DLT (Table 2: ref_accounting_treatment) with Parametrization
CREATE LIVE TABLE ref_accounting_treatment    -- TO DO --
COMMENT "Lookup mapping for accounting codes"
AS SELECT * FROM delta.`${input_data}/ref_accounting_treatment/` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Silver layer: joining tables while ensuring data quality
-- MAGIC 
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-3.png" width="500"/>
-- MAGIC 
-- MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` spacename. 
-- MAGIC 
-- MAGIC To consume only increment from the Bronze layer like `raw_txs`, we'll be using the `stream` keyworkd: `stream(LIVE.raw_txs)`
-- MAGIC 
-- MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
-- MAGIC 
-- MAGIC #### Expectations
-- MAGIC 
-- MAGIC DLT currently supports three modes for expectations:
-- MAGIC 
-- MAGIC | Mode | Behavior |
-- MAGIC | ---- | -------- |
-- MAGIC | `EXPECT` in SQL or `@dlt.expect` in Python  | Record metrics for percentage of records that fulfill expectation <br> (**NOTE**: this metric is reported for all execution modes) |
-- MAGIC | `EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE` in SQL or `@dlt.expect_or_fail` in Python| Fail the pipeline when expectation is not met |
-- MAGIC | `EXPECT (criteria) ON VIOLATION DROP ROW` in SQL or `@dlt.expect_or_drop` in Python| Only process records that fulfill expectations |
-- MAGIC 
-- MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

-- COMMAND ----------

-- DBTITLE 1,Perform ETL & Enforce Quality Expectations
CREATE STREAMING LIVE TABLE cleaned_new_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2020-12-31')) ON VIOLATION DROP ROW, -- TO DO --
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,    
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE         -- TO DO --
  -- Roadmap: Quarantine
)
COMMENT "Livestream of new transactions, cleaned and compliant"
TBLPROPERTIES ("quality" = "silver")
AS SELECT txs.*, rat.id as accounting_treatment FROM stream(LIVE.raw_txs) txs
INNER JOIN live.ref_accounting_treatment rat ON txs.accounting_treatment_id = rat.id

-- COMMAND ----------

-- DBTITLE 1,Quarantine Data with Expectations
CREATE STREAMING LIVE TABLE quarantined_cleaned_new_txs
(
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2020-12-31')),   -- TO DO --
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0),
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) 
)
TBLPROPERTIES 
("quality"="silver",
"pipelines.autoOptimize.managed"="true",
"pipelines.autoOptimize.zOrderCols"="CustomerID,InvoiceNo",
"pipelines.trigger.interval"="1 hour"
 )
AS SELECT txs.*, rat.id as accounting_treatment FROM stream(LIVE.raw_txs) txs
INNER JOIN live.ref_accounting_treatment rat ON txs.accounting_treatment_id = rat.id;

-- COMMAND ----------

-- DBTITLE 1,Historical Loan Transactions
-- CREATE LIVE TABLE historical_txs
-- COMMENT "Historical loan transactions"
-- TBLPROPERTIES ("quality" = "silver")
-- AS SELECT a.* FROM LIVE.reference_loan_stats a
-- INNER JOIN LIVE.ref_accounting_treatment b USING (id)

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC 
-- MAGIC ## Gold layer
-- MAGIC 
-- MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-4.png" width="500"/>
-- MAGIC 
-- MAGIC Our last step is to materialize the Gold Layer.
-- MAGIC 
-- MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`, and DLT will handle the rest.

-- COMMAND ----------

CREATE LIVE TABLE total_loan_balances_1
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "location_code"
)
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM live.historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

CREATE LIVE TABLE total_loan_balances_2
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES ("quality" = "gold")
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM live.historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Live Views
-- MAGIC 
-- MAGIC Views have the same update guarantees as live tables, but the results of queries are not stored to disk. 
-- MAGIC Unlike views used elsewhere in Databricks, DLT views are not persisted to the metastore, meaning that they can only be referenced from within the DLT pipeline they are a part of. 
-- MAGIC 
-- MAGIC - Views are virtual, more like a name for a query
-- MAGIC 
-- MAGIC - Use views to break up large/complex queries
-- MAGIC 
-- MAGIC - Expectations on views validate correctness of intermediate results (data quality metrics for views are not designed to be observable in UI)
-- MAGIC 
-- MAGIC - Views are recomputed every time they are required

-- COMMAND ----------

CREATE LIVE VIEW new_loan_balances_by_cost_center
COMMENT "Live voew of new loan balances for consumption by different cost centers"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "cost_center_code"
)
AS SELECT sum(balance), cost_center_code
FROM live.cleaned_new_txs
GROUP BY cost_center_code

-- COMMAND ----------

CREATE LIVE VIEW new_loan_balances_by_country     -- TO DO --
COMMENT "Live view of new loan balances per country"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "country_code"
)
AS SELECT sum(count), country_code
FROM live.cleaned_new_txs
GROUP BY country_code

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Enriching the gold data with a ML model
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-dlt-step5.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC Our Data scientist team has build a customer segmentation model and saved it into Databricks Model registry. 
-- MAGIC 
-- MAGIC We can easily load this model and enrich our data with our customer segment information. Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.

-- COMMAND ----------

-- DBTITLE 1,Load the Model as SQL Function
-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC #                                                                                         Stage/version    output
-- MAGIC #                                                                 Model name                     |            |
-- MAGIC #                                                                     |                          |            |
-- MAGIC get_cluster_udf = mlflow.pyfunc.spark_udf(spark, "models:/field_demos_customer_segmentation/Production", "string")
-- MAGIC spark.udf.register("get_customer_segmentation_cluster", get_cluster_udf)

-- COMMAND ----------

-- DBTITLE 1,Calling our ML model
CREATE STREAMING LIVE TABLE user_segmentation_dlt
COMMENT "Customer segmentation generated with our model from MLFlow registry"
AS SELECT *, get_customer_segmentation_cluster(age, annual_income, spending_core) AS segment FROM STREAM(live.user_gold_dlt)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Our pipeline is now ready!
-- MAGIC 
-- MAGIC Open the DLT menu, create a pipeline and select this notebook to run it. To generate sample data, please run the [companion notebook]($./00-Loan-Data-Generator) (make sure the path where you read and write the data are the same!)
-- MAGIC 
-- MAGIC Datas Analyst can start using DBSQL to analyze data and track our Loan metrics.  Data Scientist can also access the data to start building models to predict payment default or other more advanced use-cases.

-- COMMAND ----------

-- MAGIC %md ## Tracking data quality
-- MAGIC 
-- MAGIC Expectations stats are automatically available as system table.
-- MAGIC 
-- MAGIC This information let you monitor your data ingestion quality. 
-- MAGIC 
-- MAGIC You can leverage DBSQL to request these table and build custom alerts based on the metrics your business is tracking.
-- MAGIC 
-- MAGIC 
-- MAGIC See [how to access your DLT metrics]($./03-Log-Analysis)
-- MAGIC 
-- MAGIC <img width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC 
-- MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard example</a>
