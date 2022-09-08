-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify ETL with Delta Live Table
-- MAGIC 
-- MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
-- MAGIC 
-- MAGIC <img style="float:right" src="https://raw.githubusercontent.com/morganmazouchi/Tech-Summit-2022/main/InputData%20%26%20ML%20model/Techsummit%20DLT%202022-loanpipeline.png" width="700"/>
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
-- MAGIC <img style="float: right; padding-left: 10px" src="https://raw.githubusercontent.com/morganmazouchi/Tech-Summit-2022/main/InputData%20%26%20ML%20model/Techsummit%20DLT%202022%20-%20autoloader.png" width="800"/>
-- MAGIC 
-- MAGIC Our raw data is being sent to a cloud storage. 
-- MAGIC 
-- MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
-- MAGIC 
-- MAGIC Autoloader is available in SQL using the `cloud_files` function and can be used with a variety of format (json, csv, avro...):
-- MAGIC 
-- MAGIC 
-- MAGIC #### STREAMING LIVE TABLE 
-- MAGIC Defining tables as `STREAMING` will guarantee that you only consume new incoming data. 
-- MAGIC 1. Compute results over append-only streams such as Kafka, Kinesis, or Autoloader (files on cloud storage)
-- MAGIC 
-- MAGIC 2. Delta tables with delta.appendOnly=true
-- MAGIC 
-- MAGIC 3. Produce results on demand: 
-- MAGIC 
-- MAGIC   - Lower latency: more frequent less data processing
-- MAGIC   - Lower costs: by avoiding redundant reprocessing
-- MAGIC     
-- MAGIC     
-- MAGIC #### Data sources:
-- MAGIC 
-- MAGIC During this hands on lab, we are using 2 datasources. 
-- MAGIC One is a databricks dataset stored in  "/databricks-datasets/lending-club-loan-stats/LoanStats_*", while the second one is generated by faker library, and can be found in "/home/techsummit/dlt". Please note you only need to pass these two locations to the configuration of your pipeline when creating your pipeline. Run command 3, you can safely use the JSON configuration output for your pipeline creation.

-- COMMAND ----------

-- DBTITLE 1,Please run this cell in the notebook to get your custom DLT JSON configuration
-- MAGIC %python
-- MAGIC def print_pipeline_configuration():
-- MAGIC   import re
-- MAGIC   import json
-- MAGIC 
-- MAGIC   current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
-- MAGIC   if current_user.rfind("@") > 0:
-- MAGIC     current_user_no_at = current_user[:current_user.rfind('@')]
-- MAGIC   else:
-- MAGIC     current_user_no_at = current_user
-- MAGIC   current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)
-- MAGIC 
-- MAGIC   if dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() == "e2-demo-field-eng.cloud.databricks.com":
-- MAGIC     pool = "0727-104344-hauls13-pool-uftxk0r6"
-- MAGIC   elif dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() == "cse2.cloud.databricks.com":
-- MAGIC     pool = "0831-121407-yeahs13-pool-rym2rkke"
-- MAGIC   else:
-- MAGIC     pool = None
-- MAGIC 
-- MAGIC   path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
-- MAGIC   path = path[:path.rfind("/")]
-- MAGIC 
-- MAGIC   conf = {
-- MAGIC         "clusters": [
-- MAGIC             {
-- MAGIC                 "label": "default",
-- MAGIC                 "num_workers": 1,
-- MAGIC                 "instance_pool_id": pool,
-- MAGIC                 "driver_instance_pool_id": pool
-- MAGIC             }
-- MAGIC         ],
-- MAGIC       "development": True,
-- MAGIC       "continuous": False,
-- MAGIC       "channel": "PREVIEW",
-- MAGIC       "edition": "ADVANCED",
-- MAGIC       "photon": False,
-- MAGIC       "libraries": [
-- MAGIC           {
-- MAGIC               "notebook": {
-- MAGIC                   "path": path+"/1a-DLT-Loan-pipeline-SQL"
-- MAGIC               }
-- MAGIC           },
-- MAGIC           {
-- MAGIC               "notebook": {
-- MAGIC                   "path": path+"/1b-SQL-Delta-Live-Table-Python-UDF"
-- MAGIC               }
-- MAGIC           }
-- MAGIC       ],
-- MAGIC       "name": f"dlt-summit-step2-{current_user_no_at}",
-- MAGIC       "storage": f"/techsummit/dlt/storage/{current_user_no_at}",
-- MAGIC       "configuration": {
-- MAGIC           "loanStats": "/databricks-datasets/lending-club-loan-stats/LoanStats_*",
-- MAGIC           "input_data": "/home/techsummit/dlt"
-- MAGIC       },
-- MAGIC       "target": f"dlt_techsummit_{current_user_no_at}"
-- MAGIC   }
-- MAGIC   
-- MAGIC   print("---------------------------------------------------------------------------------")
-- MAGIC   print("MAKE SURE YOU RUN THIS CELL TO GET A CONFIGURATION WITH YOUR USER NAME AND FOLDER")
-- MAGIC   print("---------------------------------------------------------------------------------")
-- MAGIC   print(json.dumps(conf, indent=4))
-- MAGIC print_pipeline_configuration()

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data in DLT via Autoloader (Table 1: raw_txs) with Parameterization
 -- TODO: Create a streaming table and name it raw_txs  --
<FILL_IN>  
COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * 
FROM cloud_files('${input_data}/landing', 'json', map("cloudFiles.schemaEvolutionMode", "rescue")); 
-- TODO: add input_data to pipeline configuration setting using /home/techsummit/dlt

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data in DLT via Autoloader (Table 2: reference_loan_stats) + Optimize Data Layout for Performance
CREATE STREAMING LIVE TABLE reference_loan_stats
COMMENT "Raw historical transactions"
TBLPROPERTIES --Can be spark, delta, or DLT confs
("quality"="bronze",
"pipelines.autoOptimize.managed"="true",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo",
"pipelines.trigger.interval"="1 hour"
 )
AS SELECT * FROM cloud_files('${loanStats}', 'csv', map("cloudFiles.inferColumnTypes", "true"));  
-- TODO: set loanStats to pipeline configuration setting using: /databricks-datasets/lending-club-loan-stats/LoanStats_*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### LIVE TABLES 
-- MAGIC 
-- MAGIC - It scan and ingest all the data available at once. 
-- MAGIC - Always "correct", meaning their contents will match their definition after any update.
-- MAGIC 
-- MAGIC - Today they are computed completely, but we are going to get much more clever as time goes on (go/enzyme) . 

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data Stored in Delta Tables in DLT (Table 3: ref_accounting_treatment) with Parametrization
-- TODO: Create a table called ref_accounting_treatment that will be a batch load of the delta table located at "${input_data}/ref_accounting_treatment/", and name it ref_accounting_treatment
<FILL_IN> 
COMMENT "Lookup mapping for accounting codes"
<FILL_IN> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Silver layer: joining tables while ensuring data quality
-- MAGIC 
-- MAGIC <img style="float:right"  src="https://raw.githubusercontent.com/morganmazouchi/Tech-Summit-2022/main/InputData%20%26%20ML%20model/Techsummit%20DLT%202022%20-%20Silver%20Layer.png" width="500"/>
-- MAGIC 
-- MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` namespace. 
-- MAGIC 
-- MAGIC To consume only increment from the Bronze layer like `raw_txs`, we'll be using the `stream` keyword: `stream(LIVE.raw_txs)`
-- MAGIC 
-- MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
-- MAGIC 
-- MAGIC #### Expectations
-- MAGIC 
-- MAGIC DLT currently supports three modes for expectations:
-- MAGIC 
-- MAGIC | Mode | Behavior |
-- MAGIC | ---- | -------- |
-- MAGIC | `EXPECT(criteria)` in SQL or `@dlt.expect` in Python  | Record metrics for percentage of records that violate expectation <br> (**NOTE**: this metric is reported for all execution modes) |
-- MAGIC | `EXPECT (criteria) ON VIOLATION FAIL UPDATE` in SQL or `@dlt.expect_or_fail` in Python| Fail the pipeline when expectation is not met |
-- MAGIC | `EXPECT (criteria) ON VIOLATION DROP ROW` in SQL or `@dlt.expect_or_drop` in Python| Only process records that fulfill expectations |
-- MAGIC 
-- MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

-- COMMAND ----------

-- DBTITLE 1,Perform ETL & Enforce Quality Expectations
CREATE STREAMING LIVE TABLE cleaned_new_txs (
  -- TODO: Add constraint to remove any records that next_payment_date is less to date('2021-12-31') --
  <FILL_IN>
  CONSTRAINT `Balance should be positive` EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,    
  -- TODO: Add constraint that fails the entire update upon observing a null value in cost_center_code field --
  <FILL_IN>
)
COMMENT "Livestream of new transactions, cleaned and compliant"
TBLPROPERTIES ("quality" = "silver")
AS SELECT txs.*, rat.id as accounting_treatment FROM stream(LIVE.raw_txs) txs
INNER JOIN live.ref_accounting_treatment rat ON txs.accounting_treatment_id = rat.id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You’ve defined expectations to filter out records that violate data quality constraints, but you also want to save the invalid records for analysis. Create rules that are the inverse of the expectations you’ve defined and use those rules to save the invalid records to a separate table. 

-- COMMAND ----------

-- DBTITLE 1,Quarantine Data with Expectations
CREATE STREAMING LIVE TABLE quarantined_cleaned_new_txs
(
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date <= date('2021-12-31')) ON VIOLATION DROP ROW,   
  CONSTRAINT `Balance should be positive`    EXPECT (balance <= 0 AND arrears_balance <= 0) ON VIOLATION DROP ROW,
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NULL) ON VIOLATION DROP ROW
)
COMMENT "Livestream of quarantined invalid records"
TBLPROPERTIES 
("quality"="silver")
AS SELECT txs.*, rat.id as accounting_treatment FROM stream(LIVE.raw_txs) txs
INNER JOIN live.ref_accounting_treatment rat ON txs.accounting_treatment_id = rat.id;

-- COMMAND ----------

-- DBTITLE 1,Historical Loan Transactions
CREATE LIVE TABLE historical_txs
COMMENT "Historical loan transactions"
TBLPROPERTIES ("quality" = "silver")
AS SELECT a.* FROM LIVE.reference_loan_stats a
INNER JOIN LIVE.ref_accounting_treatment b USING (id);

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC 
-- MAGIC ## Gold layer
-- MAGIC 
-- MAGIC 
-- MAGIC Our last step is to materialize the Gold Layer.
-- MAGIC 
-- MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `pipelines.autoOptimize.zOrderCols`
-- MAGIC 
-- MAGIC <img style="float: center; padding-left: 50px" src="https://raw.githubusercontent.com/morganmazouchi/Tech-Summit-2022/main/InputData%20%26%20ML%20model/Techsummit%20DLT%202022%20-%20GoldLayer.png" width="800"/>

-- COMMAND ----------

CREATE LIVE TABLE total_loan_balances_1
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "location_code"
)
AS SELECT sum(revol_bal)  AS bal,
addr_state   AS location_code 
FROM live.historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal, 
country_code AS location_code
FROM live.cleaned_new_txs GROUP BY country_code;

-- COMMAND ----------

CREATE LIVE TABLE total_loan_balances_2
COMMENT "Combines historical and new loan data for unified rollup of loan balances"
TBLPROPERTIES ("quality" = "gold")
AS SELECT sum(revol_bal)  AS bal,
addr_state   AS location_code 
FROM live.historical_txs  GROUP BY addr_state
UNION SELECT sum(balance) AS bal,
country_code AS location_code 
FROM live.cleaned_new_txs GROUP BY country_code;

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
-- MAGIC - Views are not persisted, and fully recomputed when invoked

-- COMMAND ----------

CREATE LIVE VIEW new_loan_balances_by_country     
COMMENT "Live view of new loan balances per country"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "country_code"
)
AS SELECT sum(count), country_code
FROM live.cleaned_new_txs
GROUP BY country_code

-- COMMAND ----------

-- TODO: Create a view of total balance (sum) per each cost_center_code from the cleaned_new_txs table and call it new_loan_balances_by_cost_center --
 <FILL_IN> 
COMMENT "Live view of new loan balances for consumption by different cost centers"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "cost_center_code"
)
AS SELECT sum(balance), cost_center_code
FROM <FILL_IN> 
GROUP BY cost_center_code

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Enriching data with a ML model
-- MAGIC 
-- MAGIC Our Data scientist team has build a loan risk analysis model and saved it into Databricks Model registry. 
-- MAGIC 
-- MAGIC We can easily load this model and enrich our data with our loan analysis information. Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.

-- COMMAND ----------

-- DBTITLE 1,Load the Model as SQL Function in a Separate Notebook!
-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC #                                                                                           output
-- MAGIC #                                                                 Model name    stage          |
-- MAGIC #                                                                     |          |             |
-- MAGIC loan_risk_pred_udf = mlflow.pyfunc.spark_udf(spark, "models:/mlflow-loan-risk/Production", "string")
-- MAGIC spark.udf.register("loan_risk_prediction", loan_risk_pred_udf)

-- COMMAND ----------

-- DBTITLE 1,Calling our ML model
CREATE STREAMING LIVE TABLE risk_prediction_dlt
COMMENT "Risk prediction generated with our model from MLFlow registry"
AS SELECT *, 
loan_risk_prediction(struct(term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, annual_inc, delinq_2yrs, total_acc)) as pred 
FROM STREAM(live.reference_loan_stats)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Now it is time to create a pipeline!
-- MAGIC 
-- MAGIC ##### Note: You need to include the second python notebook in addition to this notebook in your pipeline!
-- MAGIC 
-- MAGIC Navigate to the workflows in the menu, switch to delta live tables and click on create a pipeline! 
-- MAGIC * Note you need to add data location paths as two parameters in your pipeline configuration:
-- MAGIC 
-- MAGIC       - key: loanStats , value: /databricks-datasets/lending-club-loan-stats/LoanStats_*
-- MAGIC       - key: input_data, value: /home/techsummit/dlt
-- MAGIC     
-- MAGIC * For this hands on session, please disable autoscale and only use 1 worker.

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
