# ML_End2End_demo

version: 1.0.0

Databricks Machine Learning を体験するデモシナリオになります。
以下の機能を体験できます。
- Databricks Notebook でのEDA (bamboolib, pyspark pandas etc) 
- Feature Store
- AutoML
- MLflow
- Model Serving 
- Batch Infer 
- Dashboard
- Alert
- Workflow 


# 使い方
1. git repogitoryを Databrricks Reposにクローンする
2. 0_setup を開き、 Prefix名を好きな名前に変更 (Database名やモデル名などに利用されます）
3. Cluster (11.2 ML Runtime 以降) を作成し起動する
4. 01_create_DeltaLake のノートブックから順次実行する

# 注意

1. Feature Storeが Unity Catalogに対応していないため、このデモではHivemetastoreを利用しております。
1. MLflow2.0 を利用する場合、DBR12以上が必要です。もしくは pip install mlflow==2.0.0 を最初に実行する必要があります。（4.バッチ推論)ノートブックではpip installを実行しておりますがDBR12がリリースされた場合、不要なオプションになります。
