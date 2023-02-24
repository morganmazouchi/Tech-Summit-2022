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

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *, loan_risk_prediction(struct(term, home_ownership, purpose, addr_state, verification_status, application_type, loan_amnt, annual_inc, delinq_2yrs, total_acc)) as pred 
# MAGIC FROM loan_stats
