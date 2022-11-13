# Databricks notebook source
# MAGIC %md # 06 Workflowによる自動化
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/handsOn/workshop_quickstart/workflow.png' width='800' />

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### 1. 新規ジョブの作成
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/workshop2022/20220623-quickstart-workshop/workflow1.png' width='800' />

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 以下の２つのノートブックをタスクとして登録し、新規のワークフローを作成します。
# MAGIC - 01_create_DeltaLake　
# MAGIC - 06_batch推論