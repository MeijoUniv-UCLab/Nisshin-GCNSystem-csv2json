import json
import calendar
import configparser
from datetime import datetime
from zoneinfo import ZoneInfo
from pymongo import MongoClient
from bson import json_util

config = configparser.ConfigParser()
config.read('config.ini')
print('configファイルを読み取りました')

# MongoDBの接続情報
MONGO_URI = config["MongoDB"]['url']
# MONGO_URI
MONGO_DB_NAME = config['MongoDB']['db_name']
# mongodb-gcp.json
MONGO_COLLECTION_NAME = config['MongoDB']['gcp']
# mognodb-multiple.json
MONGO_COLLECTION_NAME1 = config['MongoDB']['multiple']
# mongodb-dow_garbage.json
MONGO_COLLECTION_NAME2 = config['MongoDB']['dow_garbage']
# 可視化用データベース名
MONGO_COLLECTION_NAME3 = config['MongoDB']['visualization']

# mongodbからアクセスしたファイルをjson形式で読み込む
def get_json_data(collection):
    cursor = collection.find()
    # カーソルから文書を取得し、BSON から JSON に変換
    json_data = json_util.dumps(list(cursor))
    # JSON 文字列を Python オブジェクトにパース
    return json.loads(json_data)

# 曜日を数字に置き換えるための辞書
day_to_number = {
    "月": 0,
    "火": 1,
    "水": 2,
    "木": 3,
    "金": 4,
    " ": 5,
}

# 値を数字に変換
def convert_day(day):
    if isinstance(day, list):
        return [day_to_number[d] for d in day]
    return day_to_number[day]

# jsonファイルの値を数字に変換 (例)['月','水'] = [0,2]
def dow_change(data):
    # weeks = ['every', '第1', '第2', '第3', '第4']
    weeks = ['every']
    
    for week in weeks:
        data['count'][week] = convert_day(data['count'][week])
    
    return data

#### ゴミの回収を判断
# 地区を引数を利用してdow_changeに代入
def IntChange(data):
    if data[0]['region'] == "北西地区" or data[0]['region'] == "北西地区（収集日注意）":
        dow_change(data[0])
    if data[1]['region'] == "南東地区":
        dow_change(data[1])
    if data[2]['region'] == "南西地区":
        dow_change(data[2])
    if data[3]['region'] == "北東地区":
        dow_change(data[3])
    
    return data

# 曜日と週番号からその日に回収があるかどうか判断する関数
def isCount(data, dow, week_number):
    week_keys = {1: '第1', 2: '第2', 3: '第3', 4: '第4'}

    if dow in data['count']['every']:
        return True
    # week_key = week_keys[week_number]

    # if isinstance(data['count'][week_key], list):
    #     return dow in data['count'][week_key]
    # else:
    #     return dow == data['count'][week_key]

# 地区を引数にして回収があるかどうか判定
def isCountGarbageCollection(region, dow, week_number, data):
    if region == "北西地区" or region == "北西地区（収集日注意）":
        if isCount(data[0], dow, week_number):
            return True
    elif region == "南東地区":
        if isCount(data[1], dow, week_number):
            return True
    elif region == "南西地区":
        if isCount(data[2], dow, week_number):
            return True
    elif region == "北東地区":
        if isCount(data[3], dow, week_number):
            return True
    else:
        return True
    
    return False
####

#### ゴミの種類を判断する関数
# 指定した曜日と週番号のリストに含まれているか
def check_and_append(data, week_key, dow):
    if isinstance(data['count'][week_key], list):
        if dow in data['count'][week_key]:
            return data['type'][week_key][dow]
    elif dow == data['count'][week_key]:
        return data['type'][week_key][dow]
    return None


# 曜日と週番号からなんのゴミなのかを判定
def TypeGarbage(data, dow, week_num):
    # dataf = []
    week_keys = {1: '第1', 2: '第2', 3: '第3', 4: '第4'}

    if dow in data['count']['every']:
        # dataf.append(data['type']['every'][dow])
        dataf = data['type']['every'][dow]

    # if week_num in week_keys:
    #     result = check_and_append(data, week_keys[week_num], dow)
    #     if result:
    #         dataf.append(result)  

    return dataf
    
# 地区をキーにしてなんの種類のゴミなのかを判定する関数へ渡す
def regionTypeGarbage(region, dow, week_num, data):
    if region == "北西地区" or region == "北西地区（収集日注意）":
        return TypeGarbage(data[0], dow, week_num)
    elif region == "南東地区":
        return TypeGarbage(data[1], dow, week_num)
    elif region == "南西地区":
        return TypeGarbage(data[2], dow, week_num)
    elif region == "北東地区":
        return TypeGarbage(data[3], dow, week_num)
    
    return False
####

# 週番号を算出する
def get_nth_week(day):
    return (day - 1) // 7 + 1

# 週番号と曜日を算出する
def get_nth_dow(year, month, day):
    return get_nth_week(day), calendar.weekday(year, month, day)

# 曜日の変数dowから文字列の曜日を入手する
def get_day_of_week(dow):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    return days[dow]



def lambda_handler(event, context):
    try:
        client = MongoClient(MONGO_URI, connectTimeoutMS=30000, socketTimeoutMS=45000, serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        print('ok')
        collection1 = db[MONGO_COLLECTION_NAME1]
        collection2 = db[MONGO_COLLECTION_NAME2]
        collection3 = db[MONGO_COLLECTION_NAME3]

        print("MongoDBへの接続完了")

        gcp_data = get_json_data(collection)
        multiple_data = get_json_data(collection1)
        dow_garbage_data = get_json_data(collection2)
        print("MongoDBのファイルーjson形式での読み込み完了")

        
        # dow_garbage_dataの変換
        converted_dow_garbage_data = IntChange(dow_garbage_data)
        current_time = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%H:%M:%S")
        print(f"現在の時刻: {current_time}")
        
        print("可視化用データベースの作成開始")
        features = []

        for row in gcp_data:
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': row['geometry']['coordinates'],
                },
                'properties': {
                    'PointId': row['properties']['UserID'],
                    'LedgerNo': row['properties']['LedgerNo'][1:],
                    'DistrictCode': row['properties']['DistrictCode'],
                    'Location': row['properties']['Location'],
                    'Map': row['properties']['Map'],
                    'Region': row['properties']['Region'],
                }     
            }

            # 今日の日付を取得
            jst = ZoneInfo("Asia/Tokyo")
            today = datetime.now(jst).date()
            region = row.get('properties',{}).get('Region', None)

            # 週番号、曜日を取得
            year, month, day = today.year, today.month, today.day
            week_num, dow = get_nth_dow(year, month, day)
            
            # 地区制、曜日、週番号を引数にして回収回数を返す関数
            if isCountGarbageCollection(region, dow, week_num, converted_dow_garbage_data):
                feature['properties']['collectionDay'] = True 
                feature['properties']['garbageType'] = regionTypeGarbage(region, dow, week_num, dow_garbage_data)

                # Geofencingの判定用
                feature['properties']['Geofencing'] = {}
                feature['properties']['Geofencing']['Count'] = 1
                # CNNの判定用
                feature['properties']['CNN'] = {}
                feature['properties']['CNN']['Count'] = 1
                # Geofenging と CNN のOR判定用
                feature['properties']['Count'] = 1
            else:
                feature['properties']['collectionDay'] = False
                feature['properties']['garbageType'] = ''

                # Geofencingの判定用
                feature['properties']['Geofencing'] = {}
                feature['properties']['Geofencing']['Count'] = 0
                # CNNの判定用
                feature['properties']['CNN'] = {}
                feature['properties']['CNN']['Count'] = 0
                # Geofenging と CNN のOR判定用
                feature['properties']['Count'] = 0
            
            current_day_of_week = get_day_of_week(dow)
            # Database_multiple.jsonのデータを利用して回収回数を更新
            pointid = row.get('properties',{}).get('UserID',None)

            for source_data_row in multiple_data:
                if int(source_data_row['PointID']) == pointid:
                    if source_data_row.get('Schedule',{}).get(current_day_of_week, '') == 2:
                        # Geofencingの判定用
                        feature['properties']['Geofencing'] = {}
                        feature['properties']['Geofencing']['Count'] = 2
                        # CNNの判定用
                        feature['properties']['CNN'] = {}
                        feature['properties']['CNN']['Count'] = 2
                        # Geofenging と CNN のOR判定用
                        feature['properties']['Count'] = 2

                    break

            # Geofencingの判定用
            feature['properties']['Geofencing']['Status'] = 'uncollected'
            feature['properties']['Geofencing']['Date'] = ''
            feature['properties']['Geofencing']['Time'] = ''

            # CNNの判定用
            feature['properties']['CNN']['Status'] = 'uncollected'
            feature['properties']['CNN']['Date'] = ''
            feature['properties']['CNN']['Time'] = ''

            # Geofenging と CNN のOR判定用
            feature['properties']['Status'] = 'uncollected'
            feature['properties']['Date'] = ''
            feature['properties']['Time'] = ''

            # features.append(feature)
            if '可燃' in row['properties']['ItemsForCollection']:
                features.append(feature)
        print(today)
        print("可視化用データベース作成完了")

        print("データの更新開始")
        # 既存のデータを削除
        if collection3.count_documents({'type': 'Feature'}) > 0:
            collection3.delete_many({'type': 'Feature'})
            print('既存のデータを削除しました')
        else:
            print('更新すべきデータがありません')
        
        # 新しいデータを挿入
        collection3.insert_many(features)
        print("データの更新完了")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully processed and updated in MongoDB')
        }
    
    except Exception as e:
        print(f"MongoDBの接続でエラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing data: {str(e)}')
        }
    
    finally:
        # MongoDB接続を閉じる
        client.close()