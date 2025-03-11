# import json
# import urllib.parse
# import boto3
# import io
# import pandas

# print('Loading nisshin-gcnsystem-gcp-csv2geojson')

# def lambda_handler(event, context):
#     # S3からのイベントを取得
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     csv_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     #csv_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='CP932')
    
#     try:
#         # S3オブジェクトのダウンロード
#         s3 = boto3.resource('s3')
#         print("s3")
#         csv_object = s3.Object(bucket, csv_key)
#         print("csv-object")
#         csv_data = csv_object.get()['Body'].read().decode('utf-8')
#         print("data")
#         # csv_data = csv_object.get()['Body'].read().decode('CP932')
        
#         # CSVデータをDataFrameに変換
#         df = pandas.read_csv(io.StringIO(csv_data), lineterminator='\n', na_filter=False)
        
#         # DataFrameをGeoJSON形式に変換１
#         features = []
#         for _, row in df.iterrows():
#             feature = {
#                 'type': 'Feature',
#                 'geometry': {
#                     'type': 'Point',
#                     'coordinates': [row['lon'], row['lat']]
#                     # 'coordinates': [row['X'], row['Y']]
#                 },
#                 'properties': {
#                     'UserID': row['UserID'],
#                     'CoodinateSystemNo': row['CoodinateSystemNo'],
#                     'HasAttribute': row['HasAttribute'],
#                     'LedgerNo': row['台帳番号'],
#                     'DistrictCode': row['地区コード'],
#                     'Location': row['所在地'],
#                     'Map': row['地図'],
#                     'DistrictClassification': row['行政区分'],
#                     'CurrentClassification': row['現況区分'],
#                     'isUsed': row['使用の有無'],
#                     'Applicant': row['申請者'],
#                     'ApplicationDate': row['申請日'],
#                     'StartDate': row['開始日'],
#                     'EndDate': row['廃止日'],
#                     'Region': row['地区割'],
#                     'ItemsForCollection': row['収集品目'],
#                     'CollectionPointType': row['集積所形態'],
#                     'Net': row['ネット区分'],
#                     'Signboard': row['看板区分'],
#                     'LandClassification': row['土地区分'],
#                     'Owner': row['所有者'],
#                     'OwnerAddress': row['所有者住所'],
#                     'CollectionPointArea': row['集積所面積'],
#                     'CollectionPointDimension': row['集積所寸法'],
#                     'Occupation': row['占用の有無'],
#                     'OccupationPeriod': row['占用期間'],
#                     'Note1': row['備考欄1'],
#                     'Note2': row['備考欄2'],
#                     'LINKID': row['LINKID'],
#                     'PictureLink': row['リンク先'],
#                     # 'MapCode': row['地図表示CD']
#                 }
#             }
#             features.append(feature)
        
#         # S3にMongoDBにインポートするJSONファイルをアップロード
#         json_key = csv_key.replace('.csv', '-mongodb.json')
#         json_object = s3.Object(bucket, json_key)
#         json_object.put(Body=json.dumps(features, ensure_ascii=False))
        
#         # S3にGeoJSONファイルをアップロード
#         geojson = {
#             'type': 'FeatureCollection',
#             'features': features
#         }
#         geojson_key = csv_key.replace('.csv', '.geojson')
#         geojson_object = s3.Object(bucket, geojson_key)
#         geojson_object.put(Body=json.dumps(geojson, indent=2, ensure_ascii=False))
#     except Exception as e:
#         print(e)
#         raise e
        
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }

# import json
# import urllib.parse
# import boto3
# import io
# import pandas
# import configparser
# from pymongo import MongoClient

# print('Loading nisshin-gcnsystem-gcp-csv2geojson')

# config = configparser.ConfigParser()
# config.read('config.ini')
# print('configファイルを読み取りました')

# #MongoDBの接続情報
# MONGO_URI = config["MongoDB"]['url']
# # MONGO_URI
# MONGO_DB_NAME = config['MongoDB']['db_name']
# #mongodb-gcp.json
# MONGO_COLLECTION_NAME = config['MongoDB']['gcp']

# def lambda_handler(event, context):
#     # S3からのイベントを取得
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     csv_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
#     try:
#         # S3オブジェクトのダウンロード
#         print('s3オブジェクトのダウンロード開始')
#         s3 = boto3.resource('s3')
#         csv_object = s3.Object(bucket, csv_key)
#         csv_data = csv_object.get()['Body'].read().decode('utf-8')
       
        
#         # CSVデータをDataFrameに変換
#         print('CSVデータをDataFrameに変換')
#         df = pandas.read_csv(io.StringIO(csv_data), lineterminator='\n', na_filter=False)
        
#         # DataFrameをGeoJSON形式に変換
#         features = []
#         for _, row in df.iterrows():
#             feature = {
#                 'type': 'Feature',
#                 'geometry': {
#                     'type': 'Point',
#                     'coordinates': [row['lon'], row['lat']]
#                     # 'coordinates': [row['X'], row['Y']]
#                 },
#                 'properties': {
#                     'UserID': row['UserID'],
#                     'CoodinateSystemNo': row['CoodinateSystemNo'],
#                     'HasAttribute': row['HasAttribute'],
#                     'LedgerNo': row['台帳番号'],
#                     'DistrictCode': row['地区コード'],
#                     'Location': row['所在地'],
#                     'Map': row['地図'],
#                     'DistrictClassification': row['行政区分'],
#                     'CurrentClassification': row['現況区分'],
#                     'isUsed': row['使用の有無'],
#                     'Applicant': row['申請者'],
#                     'ApplicationDate': row['申請日'],
#                     'StartDate': row['開始日'],
#                     'EndDate': row['廃止日'],
#                     'Region': row['地区割'],
#                     'ItemsForCollection': row['収集品目'],
#                     'CollectionPointType': row['集積所形態'],
#                     'Net': row['ネット区分'],
#                     'Signboard': row['看板区分'],
#                     'LandClassification': row['土地区分'],
#                     'Owner': row['所有者'],
#                     'OwnerAddress': row['所有者住所'],
#                     'CollectionPointArea': row['集積所面積'],
#                     'CollectionPointDimension': row['集積所寸法'],
#                     'Occupation': row['占用の有無'],
#                     'OccupationPeriod': row['占用期間'],
#                     'Note1': row['備考欄1'],
#                     'Note2': row['備考欄2'],
#                     'LINKID': row['LINKID'],
#                     'PictureLink': row['リンク先'],
#                     # 'MapCode': row['地図表示CD']
#                 }
#             }
#             features.append(feature)
        
#         # S3にMongoDBにインポートするJSONファイルをアップロード
#         print('s3にjsonファイルをアップロード')
#         json_key = csv_key.replace('.csv', '-mongodb.json')
#         json_object = s3.Object(bucket, json_key)
#         json_object.put(Body=json.dumps(features, ensure_ascii=False))
        
#         # S3にGeoJSONファイルをアップロード
#         print('s3にgeojsonファイルをアップロード')
#         geojson = {
#             'type': 'FeatureCollection',
#             'features': features
#         }
#         geojson_key = csv_key.replace('.csv', '.geojson')
#         geojson_object = s3.Object(bucket, geojson_key)
#         geojson_object.put(Body=json.dumps(geojson, indent=2, ensure_ascii=False))
        
#         client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
#         db = client[MONGO_DB_NAME]
#         collection = db[MONGO_COLLECTION_NAME]

#         print("データの更新開始")
#         # 既存のデータを削除
#         if collection.count_documents({'type': 'Feature'}) > 0:
#             collection.delete_many({'type': 'Feature'})
#             print('既存のデータを削除しました')
#         else:
#             print('更新すべきデータがありません')
        
#         # 新しいデータを挿入
#         collection.insert_many(features)
#         print("データの更新完了")
        
#     except Exception as e:
#         print(e)
#         raise e
        
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }

import json
import urllib.parse
import boto3
import io
import pandas
import configparser
from pymongo import MongoClient

print('Loading nisshin-gcnsystem-gcp-csv2geojson')

config = configparser.ConfigParser()
config.read('config.ini')
print('configファイルを読み取りました')

#MongoDBの接続情報
MONGO_URI = config["MongoDB"]['url']
# MONGO_URI
MONGO_DB_NAME = config['MongoDB']['db_name']
#mongodb-gcp.json
MONGO_COLLECTION_NAME = config['MongoDB']['gcp']

def lambda_handler(event, context):
    # S3からのイベントを取得
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # S3オブジェクトのダウンロード
        print('s3オブジェクトのダウンロード開始')
        s3 = boto3.resource('s3')
        csv_object = s3.Object(bucket, csv_key)
        csv_data = csv_object.get()['Body'].read().decode('utf-8')
       
        
        # CSVデータをDataFrameに変換
        print('CSVデータをDataFrameに変換')
        df = pandas.read_csv(io.StringIO(csv_data), lineterminator='\n', na_filter=False)
        
        # DataFrameをGeoJSON形式に変換
        features = []
        for _, row in df.iterrows():
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [row['lon'], row['lat']]
                },
                'properties': {
                    'UserID': row['UserID'],
                    'CoodinateSystemNo': row['CoodinateSystemNo'],
                    'HasAttribute': row['HasAttribute'],
                    'LedgerNo': row['台帳番号'],
                    'DistrictCode': row['地区コード'],
                    'Location': row['所在地'],
                    'Map': row['地図'],
                    'DistrictClassification': row['行政区分'],
                    'CurrentClassification': row['現況区分'],
                    'isUsed': row['使用の有無'],
                    'Applicant': row['申請者'],
                    'ApplicationDate': row['申請日'],
                    'StartDate': row['開始日'],
                    'EndDate': row['廃止日'],
                    'Region': row['地区割'],
                    'ItemsForCollection': row['収集品目'],
                    'CollectionPointType': row['集積所形態'],
                    'Net': row['ネット区分'],
                    'Signboard': row['看板区分'],
                    'LandClassification': row['土地区分'],
                    'Owner': row['所有者'],
                    'OwnerAddress': row['所有者住所'],
                    'CollectionPointArea': row['集積所面積'],
                    'CollectionPointDimension': row['集積所寸法'],
                    'Occupation': row['占用の有無'],
                    'OccupationPeriod': row['占用期間'],
                    'Note1': row['備考欄1'],
                    'Note2': row['備考欄2'],
                    'LINKID': row['LINKID'],
                    'PictureLink': row['リンク先'],
                }
            }
            #features.append(feature)
            # '地区割'が空白でない場合のみfeaturesに追加
            if feature['properties']['Region'].strip():
                features.append(feature)
                
             
        
        # S3にMongoDBにインポートするJSONファイルをアップロード
        print('s3にjsonファイルをアップロード')
        json_key = csv_key.replace('.csv', '-mongodb.json')
        json_object = s3.Object(bucket, json_key)
        json_object.put(Body=json.dumps(features, ensure_ascii=False))
        
        # S3にGeoJSONファイルをアップロード
        print('s3にgeojsonファイルをアップロード')
        geojson = {
            'type': 'FeatureCollection',
            'features': features
        }
        geojson_key = csv_key.replace('.csv', '.geojson')
        geojson_object = s3.Object(bucket, geojson_key)
        geojson_object.put(Body=json.dumps(geojson, indent=2, ensure_ascii=False))
        
        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]

        print("データの更新開始")
        # 既存のデータを削除
        if collection.count_documents({'type': 'Feature'}) > 0:
            collection.delete_many({'type': 'Feature'})
            print('既存のデータを削除しました')
        else:
            print('更新すべきデータがありません')
        
        # 新しいデータを挿入
        collection.insert_many(features)
        print("データの更新完了")
        
    except Exception as e:
        print(e)
        raise e
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

