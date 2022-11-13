# Databricks notebook source
# MAGIC %md 
# MAGIC **Requirement**  Databricks Runtime ML & 11.2以上

# COMMAND ----------

# MAGIC %md # Machine Learning End to End Demo 概要
# MAGIC 
# MAGIC 実際にRawデータから加工してモデル学習＆デプロイまで構築するデモになります。以下のようなパイプラインを想定しております。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/overall.png' width='1200'/>

# COMMAND ----------

# bamboolibを利用する場合（今回このノートブックで利用予定）
%pip install bamboolib 

# COMMAND ----------

# DBTITLE 1,Setup Script for hands-on
# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md # 01. Create Delta Lake
# MAGIC Azure Blob Storage上のcsvデータを読み込み、必要なETL処理を実施した上でデルタレイクに保存するまでのノートブックになります。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/1_createDelta.png' width='800' />

# COMMAND ----------

# MAGIC %md ## Data Load & Create Bronze Table
# MAGIC 
# MAGIC DataLake上のデータをロードして、データプロファイルをチェックしてみよう

# COMMAND ----------

# DBTITLE 1,データロード & Bronze Table の作成
df = spark.read.format('csv').option('header', True).schema(bronze_schema).load('wasbs://public-data@sajpstorage.blob.core.windows.net/')
df.write.mode('overwrite').saveAsTable("bronze_table")

# COMMAND ----------

# DBTITLE 1,データ探索をしてみよう
df_bronze = spark.read.table("bronze_table")
display(df_bronze)

# COMMAND ----------

# MAGIC %md ##（オプション） Bamboolibを使った LowCode処理が可能です。
# MAGIC 
# MAGIC ただし、現在はSparkDataFrameに対応していないため、pandas dataframeとして利用する必要があります。<br>
# MAGIC またUnityCatalogに対応したいないため、Hive Catalogを利用頂くか、別途コードで読み書きする必要があります

# COMMAND ----------

import bamboolib as bam

df = spark.table("bronze_table").toPandas()
df

# COMMAND ----------

# MAGIC %md ## PySpark Pandas API を使って前処理を実施
# MAGIC 
# MAGIC 多くの Data Scientist は、pandasの扱いになれており、Spark Dataframeには不慣れです。Spark 3.2より Pandas APIを一部サポートしました。<br>
# MAGIC これにより、Pandasの関数を使いながら、Sparkの分散機能も使うことが可能になります。 <br>
# MAGIC **Requirement** Spark3.2以降の機能なため、**DBR 10.2以上**のクラスター上で実行する必要があります

# COMMAND ----------

import pyspark.pandas as ps

# Convert to koalas
data = df_bronze.to_pandas_on_spark()

# OHE
data = ps.get_dummies(data, 
                        columns=['gender', 'partner', 'dependents',
                                 'phoneService', 'multipleLines', 'internetService',
                                 'onlineSecurity', 'onlineBackup', 'deviceProtection',
                                 'techSupport', 'streamingTV', 'streamingMovies',
                                 'contract', 'paperlessBilling', 'paymentMethod'],dtype = 'int64')

# Convert label to int and rename column
data['churnString'] = data['churnString'].map({'Yes': 1, 'No': 0})
data = data.astype({'churnString': 'int32'})
data = data.rename(columns = {'churnString': 'churn'})
  
# Clean up column names
data.columns = data.columns.str.replace(' ', '')
data.columns = data.columns.str.replace('(', '-')
data.columns = data.columns.str.replace(')', '')
  
# Drop missing values
data = data.dropna()
  
data.head(10)

# COMMAND ----------

# MAGIC %md-sandbox ## Feature Storeに保存
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC 
# MAGIC 機能の準備ができたら、Databricks Feature Storeに保存します。
# MAGIC その際、フィーチャーストアはDelta Lakeのテーブルでバックアップされます。
# MAGIC これにより、組織全体で機能の発見と再利用が可能になり、チームの効率が向上します。
# MAGIC 
# MAGIC フィーチャーストアは、デプロイメントにトレーサビリティとガバナンスをもたらし、どのモデルがどの機能のセットに依存しているかを把握することができます。
# MAGIC UIを使用してフィーチャーストアにアクセスするには、"Machine Learning "メニューを使用していることを確認してください。

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

#dbName = 'customer_churn_demo'   # Database name
featureTableName = 'churn_features'     # Table name

spark.sql(f'create database if not exists {dbName}')

#churn_feature_table = 
fs.create_table(
  name=f'{dbName}.{featureTableName}',
  primary_keys='customerID',
  schema=data.spark.schema(),
  description='これらの特徴は、外部ストレージの　customer.csvから派生したものです。 カテゴリ列に対してダミー変数を作成し、その名前をクリーンアップし、顧客が解約したかどうかのブール型フラグを追加しました。 集計は行っていません。'
)

fs.write_table(df=data.to_spark(), name=f'{dbName}.{featureTableName}', mode='overwrite')

# COMMAND ----------

