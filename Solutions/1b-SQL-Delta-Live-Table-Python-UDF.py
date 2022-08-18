# Databricks notebook source
# DBTITLE 1,Let's install mlflow to load our model
# MAGIC %pip install mlflow

# COMMAND ----------

# MAGIC %md #Registering python UDF to a SQL function
# MAGIC This is a companion notebook to use a spark udf and save it as a SQL function
# MAGIC  
# MAGIC Make sure you add this notebook in your DLT job to have access to below function. (Currently mixing python in a SQL DLT notebook won't run the python)

# COMMAND ----------

import mlflow

loan_risk_pred_udf = mlflow.pyfunc.spark_udf(spark, "models:/mlflow-loan-risk/Production", "string")
spark.udf.register("loan_risk_prediction", loan_risk_pred_udf)

# COMMAND ----------

# MAGIC %md ### Setting up the DLT 
# MAGIC 
# MAGIC Here is a setting example including the python UDF and the SQL function. 
# MAGIC 
# MAGIC * Note: use unique target (recommended target: firstname_lastname_DB)
# MAGIC * Note the multiple entries (1 per notebook) in the "libraries" option:
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC     "id": "5edee273-d5bc-423b-aa30-1f003dc423a7",
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "development": true,
# MAGIC     "continuous": false,
# MAGIC     "channel": "PREVIEW",
# MAGIC     "edition": "ADVANCED",
# MAGIC     "photon": false,
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/mojgan.mazouchi@databricks.com/Tech-Summit-2022/Solutions/1a-DLT-Loan-pipeline-SQL"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/mojgan.mazouchi@databricks.com/Tech-Summit-2022/Solutions/1b-SQL-Delta-Live-Table-Python-UDF"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "Tech-summit-step2",
# MAGIC     "storage": "/tmp/logs",
# MAGIC     "configuration": {
# MAGIC         "loanStats": "/databricks-datasets/lending-club-loan-stats/LoanStats_*",
# MAGIC         "input_data": "/home/techsummit/dlt"
# MAGIC     },
# MAGIC     "target": "TechsummitDB2"
# MAGIC }
# MAGIC ```
