# Databricks notebook source
# MAGIC %md # 03. モデルサービングによる推論
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/5_model_serving.png' width='800' />

# COMMAND ----------

# MAGIC %md ## トークンの発行
# MAGIC 
# MAGIC モデルサービング機能を使って、推論するためには、トークンを発行して置く必要があります。　<br>
# MAGIC `Setting` - `User Settings` - `Access Tokens`
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/token.png' width='800' />

# COMMAND ----------

# MAGIC %md ## Curl によるアクセス
# MAGIC 
# MAGIC サンプルデータとしてこちらの`data.json`をローカルにダウンロードする (右クリックで保存)
# MAGIC 
# MAGIC https://sajpstorage.blob.core.windows.net/public-data/data.json

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 次に、モデルサービングのUI画面にて、Curlタブのコードをコピーして、ローカル端末のターミナル上にペーストします。Tokenの箇所を先ほど発行したTokenを使って置き換えます。
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/curl.png' width='800' />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/curl2.png' width='800' /> <br>
# MAGIC (*) sudo 実行が必要な場合があります。

# COMMAND ----------

# MAGIC %md ## Python　コードによるクライアントアクセス
# MAGIC 
# MAGIC Pythonコードでも、エンドポイント経由でアクセスする事ができます。<br>
# MAGIC 
# MAGIC 以下は、クライアント側のコードで実行しているイメージでご覧ください。

# COMMAND ----------

import os

# Token をコピペ
os.environ["DATABRICKS_TOKEN"] = '<token>'

# Secretを利用したトークンの引き渡しの場合
#os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get("maru_scope", "token")

# COMMAND ----------

# MAGIC %md 
# MAGIC 次に、モデルサービングのUI画面にて、Pythonタブのコードをコピーして、下のセルにペーストします。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/model_python.png' width='800' />

# COMMAND ----------

# DBTITLE 1,こちらにペーストしてください(上書き)

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = '<databricks-uri>'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

# モデルサービングは、比較的小さいデータバッチにおいて低レーテンシーで予測するように設計されています。

sample_data = 'wasbs://public-data@sajpstorage.blob.core.windows.net/data.json'
sample_df = spark.read.format('json').load(sample_data).toPandas()

served_predictions = score_model(sample_df)
print(served_predictions)

# COMMAND ----------

pre = served_predictions['predictions']
sample_df['prediction']=pre
sample_df = sample_df.replace({'prediction': {1: "解約"}}).replace({'prediction': {0: "継続"}})
sample_df[['customerID','prediction']]

# COMMAND ----------

# MAGIC %md # Streamlit との連携
# MAGIC 
# MAGIC 上記のpython codeを実装することで、Streamlitで予測結果を出力出来るアプリを簡単に作成出来ます。
# MAGIC 
# MAGIC https://maroon-spec-streamlit-ml-demo-app-ev9cq5.streamlit.app/
# MAGIC 
# MAGIC アップロードする[サンプルデータはこちら](https://sajpstorage.blob.core.windows.net/public-data/data.json)（右クリックでダウンロードしてください）　