# Databricks notebook source
# MAGIC %md ### 事前セットアップ箇所 ###

# COMMAND ----------

# DBTITLE 1,↓↓オリジナルのプレフィックスをセットしてください↓↓
# original prefix 
#.自分のイニシャルなど、わかりやすい＆被らない名前を利用してください。
prefix = 'daiwt2022'

# COMMAND ----------

# MAGIC %md ## UC 設定
# MAGIC 
# MAGIC 管理者は、あらかじめ、ハンズオン用のカタログを作成しておく必要があります。<br>
# MAGIC メタストア管理者であることを確認の上以下のコマンドを実行して、あらかじめカタログを用意ください。<br>
# MAGIC 
# MAGIC ```
# MAGIC spark.sql('CREATE CATALOG IF NOT EXISTS daiwt22')
# MAGIC spark.sql('GRANT CREATE, USAGE ON CATALOG ml_handson TO `account users`')
# MAGIC ```

# COMMAND ----------

# MAGIC %md #### 以下は編集不要です。 ###

# COMMAND ----------

#--- use catalog
#spark.sql('USE CATALOG daiwt22')

#--- Create a new schema in the quick_start catalog
#spark.sql(f'CREATE SCHEMA IF NOT EXISTS {prefix}_schema')
#spark.sql(f'USE {prefix}_schema')

# COMMAND ----------

# Delta Lake Path 
bronze_path = f'/tmp/{prefix}/e2e_demo/bronze'
result_path = f'/tmp/{prefix}/e2e_demo/result'

# Hive Database Name
dbName = f'{prefix}_db'
spark.sql(f'CREATE DATABASE IF NOT EXISTS {dbName}')
spark.sql(f'USE DATABASE {dbName}')


# COMMAND ----------

# Bronze Table用スキーマ

bronze_schema = ''' \
customerID string,\
gender string,\
seniorCitizen double,\
partner string,\
dependents string,\
tenure double,\
phoneService string,\
multipleLines string,\
internetService string,\
onlineSecurity string,\
onlineBackup string,\
deviceProtection string,\
techSupport string,\
streamingTV string,\
streamingMovies string,\
contract string,\
paperlessBilling string,\
paymentMethod string,\
monthlyCharges double,\
totalCharges string,\
churnString string\
'''

# COMMAND ----------

#print(f'prefix: {prefix}')
#print(f'bronze_path: {bronze_path}')
#print(f'result_path: {result_path}')
print(f'dbName: {dbName}')

# COMMAND ----------

