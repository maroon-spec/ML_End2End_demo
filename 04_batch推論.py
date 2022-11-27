# Databricks notebook source
# MAGIC %md # 04 Bacth/Streaming による推論
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/6_batch_infer.png' width='800' />

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC <img style="float: right" src='https://sajpstorage.blob.core.windows.net/demo-asset-workshop2021/20210222_deployment_pattern.png' width='800' />
# MAGIC 
# MAGIC Databricks上でのモデルのデプロイは以下の3通りに分けられます。
# MAGIC 
# MAGIC 1. バッチ推論: Databricks上のnotebookでDataframeを入力し、スコアリングするコードを定期実行する
# MAGIC 1. ストリーミング推論: Databricks上のnotebookでストリーミングDataframeを入力し、スコアリングを逐次実行する
# MAGIC 1. Model Serving: REST Server上にモデルをデプロイし、HTTPリクエストでスコアリングデータを読み込み、レスポンスで推定結果を返す
# MAGIC 
# MAGIC Databricks上ではバッチ処理、ストリーミング処理がDataframe的に同等に扱えるため、上記のバッチ処理、ストリーミング処理はほぼ同じデプロイ方法になります。
# MAGIC Rest Servingについては、MLflowのレジストリUIからデプロイ可能です。

# COMMAND ----------
# MAGIC %pip install 'mlflow==2.0.0'

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md ## モデルのロード
# MAGIC 
# MAGIC mlflow には`pyfunc`というライブラリがあり、モデルをロードしてUDF化してくれる関数も用意してあります。<br> 
# MAGIC env_manager="conda" を指定すると、MLflow Modelsの conda 情報に沿って環境をセットアップしてくれます。これでモデル作成時と同じ環境を用意できます。

# COMMAND ----------

import mlflow.pyfunc

model_name = f"{prefix}_model"  # ご自分のmodel nameに変更ください
model_version = 'staging'     # model_version = 'production' ## <= このようにproduction/stagingも指定可能

# Load model from registry
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}/{model_version}", env_manager="virtualenv")

# COMMAND ----------

# MAGIC %md ## Spark DataFrameによる推論
# MAGIC 
# MAGIC Spark DataFrameの場合、バッチとストリーミングを両方とも扱える点と、SparkAPIを使った分散処理が出来るため大量のデータを非常に高速に処理実行することができます。<br>
# MAGIC ただしPandasにも対応しているためどちらも利用することが可能です。

# COMMAND ----------

# 推定を実施する(スコアリングを実施する)対象のデータを読み込む
# Feature Storeに格納されているデータを読み込み、バッチ推論を実施します。

from databricks.feature_store import FeatureStoreClient
from pyspark.sql.functions import *

fs = FeatureStoreClient()

tableName = 'churn_features'

# Read from feature store 
df = fs.read_table(f'{dbName}.{tableName}')
print(f'Total data numbers are {df.count()}')

# モデルを適用して推定する(スコアリング)
# y_test の結果(label)を予測
pred_df = df.withColumn('prediction', loaded_model(*df.columns))

display(pred_df)

# COMMAND ----------

# MAGIC %md ## Delta Lakeへ保存
# MAGIC 
# MAGIC 最後に元のデータ（Bronze Data)に予測結果を追加してDeltaLakeに保存しておきます。この後Databricks SQLによる可視化等で利用します。

# COMMAND ----------

#01のノートブックでソースデータをDeltaLakeに保存したパスを指定します。

#ソースデータを読み込みます。
bronze_df = spark.read.table("bronze_table")

#予測結果をjoinします。
final_df = bronze_df.join(pred_df.select('customerID','prediction','churn'), bronze_df.customerID == pred_df.customerID, "left").drop(pred_df.customerID)

#DeltaTableとして保存
resultTableName='churn_prediction'
final_df.write.mode('overwrite').saveAsTable(f'{dbName}.{resultTableName}')

display(final_df)

# COMMAND ----------

display(final_df)
