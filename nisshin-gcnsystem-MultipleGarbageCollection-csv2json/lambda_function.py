# ##ゴミ集積所のCSVデータをMongoDBにインポートするJSON形式に変換する関数．S3にCSVファイルがアップロードされたら，自動的に変換する．
# import json ##jsonデータの読み書きを行うための機能
# import urllib.parse ##URLを解析したり構築したりするための機能
# import boto3 ##AWSのサービスを操作するためのライブラリ(AWSのリソースを管理するためのクライアント)
# import io ##ファイルやバイトストリームなどの入出力操作を行うための機能
# import pandas ##データ解析の為のオープンソースのライブラリ(データフレームやシリーズなどのデータ構造を扱うことができる)


# print('Loading nisshin-gcnsystem-MultipleGarbageCollction-csv2json')

# def lambda_handler(event, context):
#     # S3からのイベントを取得
#     print("ダウンロード開始-key")
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     csv_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     print("ダウンロード終了-key")
    
#     try:
#         print("ダウンロード開始")
#         # S3オブジェクトのダウンロード
#         s3 = boto3.resource('s3')
#         print("s3")
#         csv_object = s3.Object(bucket, csv_key)
#         print("csv-object")
#         csv_data = csv_object.get()['Body'].read().decode('utf-8')
#         print("data")
#         print("s3オブジェクトのダウンロード完了")
        
#         # CSVデータをDataFrameに変換
#         df = pandas.read_csv(io.StringIO(csv_data), lineterminator='\n', na_filter=False)
        
#         print("データ変換を開始")
#         # DataFrameをGeoJSON形式に変換
#         data = []
#         for _, row in df.iterrows():
#             user_data = {
#                 "PointID": row['PointID'],
#                 "Schedule": {}
#             }

#             # 曜日ごとのデータを処理
#             for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
#                 if day in row and row[day]:
#                     user_data["Schedule"][day] = int(row[day])
#             data.append(user_data)

#         print("S3にファイルをアップロード")
#         # S3にMongoDBにインポートするJSONファイルをアップロード
#         json_key = csv_key.replace('.csv', '-mongodb.json')
#         json_object = s3.Object(bucket, json_key)
#         json_object.put(Body=json.dumps(data, ensure_ascii=False))


        
#     except Exception as e:
#         print(e)
#         raise e
        
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }


##ゴミ集積所のCSVデータをMongoDBにインポートするJSON形式に変換する関数．S3にCSVファイルがアップロードされたら，自動的に変換する．
import json ##jsonデータの読み書きを行うための機能
import urllib.parse ##URLを解析したり構築したりするための機能
import boto3 ##AWSのサービスを操作するためのライブラリ(AWSのリソースを管理するためのクライアント)
import io ##ファイルやバイトストリームなどの入出力操作を行うための機能
import pandas ##データ解析の為のオープンソースのライブラリ(データフレームやシリーズなどのデータ構造を扱うことができる)
import configparser
from pymongo import MongoClient

print('Loading nisshin-gcnsystem-MultipleGarbageCollction-csv2json')

config = configparser.ConfigParser()
config.read('config.ini')
print('configファイルを読み取りました')

#MongoDBの接続情報
MONGO_URI = config["MongoDB"]['url']
# MONGO_URI
MONGO_DB_NAME = config['MongoDB']['db_name']
#mongodb-gcp.json
MONGO_COLLECTION_NAME = config['MongoDB']['multiple']


def lambda_handler(event, context):
    # S3からのイベントを取得
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # S3オブジェクトのダウンロード
        print('S3オブジェクトのダウンロード')
        s3 = boto3.resource('s3')
        csv_object = s3.Object(bucket, csv_key)
        csv_data = csv_object.get()['Body'].read().decode('utf-8')
        
        # CSVデータをDataFrameに変換
        print('CSVデータをDataFrameに変換')
        df = pandas.read_csv(io.StringIO(csv_data), lineterminator='\n', na_filter=False)
        
        # DataFrameをGeoJSON形式に変換
        data = []
        for _, row in df.iterrows():
            user_data = {
                "PointID": row['PointID'],
                "Schedule": {}
            }

            # 曜日ごとのデータを処理
            for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
                if day in row and row[day]:
                    user_data["Schedule"][day] = int(row[day])
            data.append(user_data)

        
        # S3にMongoDBにインポートするJSONファイルをアップロード
        print('s3にjsonファイルをアップロード')
        json_key = csv_key.replace('.csv', '-mongodb.json')
        json_object = s3.Object(bucket, json_key)
        json_object.put(Body=json.dumps(data, ensure_ascii=False))


        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]

        print("データの更新開始")
        # 既存のデータを削除
        if collection.count_documents({'PointID' : {'$exists': True}}) > 0:
            collection.delete_many({'PointID': {'$exists': True}})
            print('既存のデータを削除しました')
        else:
            print('更新すべきデータがありません')
        
        # 新しいデータを挿入
        collection.insert_many(data)
        print("データの更新完了")


        
    except Exception as e:
        print(e)
        raise e
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }