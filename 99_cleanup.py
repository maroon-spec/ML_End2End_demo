# Databricks notebook source
# MAGIC %md # Clean up script
# MAGIC 
# MAGIC ハンズオンの最後にこちらのノートブックを実行して、環境をクリーンアップしてください。

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md ## 1) Feature Tableの削除 (From UI)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Feature Tableは現在UIから削除する必要があります。<br>
# MAGIC <img src='https://docs.databricks.com/_images/feature-store-deletion.png' width='800' />

# COMMAND ----------

# MAGIC %md ## 2) Database & Table、データ削除

# COMMAND ----------

# Drop Database & Table
spark.sql(f'drop database {dbName} cascade')

# Delete Delta Path
dbutils.fs.rm(bronze_path, True)
dbutils.fs.rm(result_path, True)

# COMMAND ----------

# MAGIC %md ## 3) Model Serving のStop

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Model Serving を有効にしている場合は Stopしてください。
# MAGIC 
# MAGIC <img src='https://docs.databricks.com/_images/serving-tab.png' width='800' />

# COMMAND ----------

# MAGIC %md ## 4) 登録されたMLflow ModelのStagin変更と削除

# COMMAND ----------

from mlflow.tracking import MlflowClient
import mlflow

model_name = f"{prefix}_churn"

model_info = MlflowClient().get_latest_versions(name=model_name, stages=['Staging'])
staging_version = model_info[0].version

client = MlflowClient()
client.transition_model_version_stage(
    name=model_name,
    version=staging_version,
    stage="Archived"
)

client.delete_registered_model(name=model_name)

# COMMAND ----------

