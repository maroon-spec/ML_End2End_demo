# Databricks notebook source
# MAGIC %md # 02 AutoMLによるモデル作成
# MAGIC 
# MAGIC このノートブックでは、01で作ったFeature Store上のデータを使ってベストなモデルを作成します。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/2_automl.png' width='800' />

# COMMAND ----------

# MAGIC %md-sandbox ### DatabricksのAuto MLとChurnデータセットの使用
# MAGIC 
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC 
# MAGIC Auto MLは、「Machine Learning(機械学習)」メニュースペースで利用できます。<br>
# MAGIC (Machine Learning メニューを選択し、ホーム画面で AutoMLを選択してください)
# MAGIC 
# MAGIC 新規にAuto-ML実験を開始し、先ほど作成した特徴量テーブル(`churn_features`)を選択するだけで良いのです。
# MAGIC 
# MAGIC ML Problem typeは、今回は`classification`です。
# MAGIC 予測対象は`churn`カラムです。
# MAGIC 
# MAGIC AutoMLのMetricや実行時間とトライアル回数については、Advance Menuで選択できます。
# MAGIC 
# MAGIC Demo時には時間短縮のため、5分にセットしてください。
# MAGIC 
# MAGIC Startをクリックすると、あとはDatabricksがやってくれます。
# MAGIC 
# MAGIC この作業はUIで行いますが[python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)による操作も可能です。

# COMMAND ----------

# MAGIC %md ## 実験の経過や結果については、MLflow のExperiment画面で確認可能です。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/automl_experiment.png' />

# COMMAND ----------

# MAGIC %md ## 注意事項
# MAGIC 
# MAGIC - AutoMLは、シングルノード上で実験が行われるため、メモリサイズが小さいと学習できるデータセット数が小さくなります。そのためメモリ搭載の多いインスタンスを選択してください。 
# MAGIC - 上図(MLflow Experiment UI)の Alertタブを確認ください。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/tutoml_alert.png' />

# COMMAND ----------

# MAGIC %md ## AutoMLが完了した後は

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/best_model.png' />

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC クローンしたベストモデルのノートブック上で、データ数を増やしたり、アラート内容の箇所を修正したりして、本番用のノートブックを作成してください。

# COMMAND ----------

# MAGIC %md # AutoML APIを使った実行はこちら

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from databricks import automl

df = spark.read.table("churn_features")

summary = (automl.classify(df, 
                           target_col="churn", 
                           timeout_minutes=10,
                           primary_metric="f1",
                          )
          )

help(summary)

# COMMAND ----------

# DBTITLE 1,Best Model をMlflow registry に登録
from mlflow import MlflowClient
import mlflow

client = MlflowClient()

model_name = f'{prefix}_model'

# Register best model to MLflow registry
model_uri = summary.best_trial.model_path
result=mlflow.register_model(model_uri, model_name)


# COMMAND ----------

# DBTITLE 1,Modelを Staging に移行
client.transition_model_version_stage(
    name=model_name,
    version=result.version,
    stage="Staging"
)

# COMMAND ----------

