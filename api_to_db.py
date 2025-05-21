from dotenv import load_dotenv
import os
import requests
from connect_db import DatabaseManager
import pandas as pd

"""
API에서 데이터를 가져오고, DB에 저장하는 파일
가져오는 데이터는 측정소 정보, 측정소별 실시간 측정정보
"""

def get_api_data(url, params):
    """API에서 데이터를 가져오는 함수"""
    load_dotenv()
    service_key = os.getenv('OPEN_API_KEY')
    decoded_service_key = requests.utils.unquote(service_key)

    params['serviceKey'] = decoded_service_key
    response = requests.get(url, params=params)
    
    try:
        data = response.json()
        items = data['response']['body']['items']
        # DataFrame으로 변환
        df = pd.DataFrame(items)
        return df
    except Exception as e:
        print("JSON 디코딩 오류:", e)
        print("응답 내용:", response.text)
        return None

def save_to_database(df, query_template, value_mapping):
    """
    DataFrame을 DB에 저장하는 함수
    
    Args:
        df (DataFrame): 저장할 데이터가 있는 DataFrame
        query_template (str): INSERT 쿼리 템플릿
        value_mapping (dict): DataFrame 컬럼명과 DB 컬럼명의 매핑
            예: {'stationName': 'station_name', 'addr': 'addr', ...}
    """
    db = DatabaseManager()
    
    try:
        # DB 연결
        db.connect()
        
        # DataFrame의 각 행을 DB에 저장
        for _, row in df.iterrows():
            # value_mapping을 기반으로 실제 값 생성
            values = tuple(row[df_col] for df_col in value_mapping.keys())
            
            db.execute_insert(query_template, values)
        
        print("데이터 저장 완료")
        
    except Exception as e:
        print(f"데이터 저장 중 오류 발생: {e}")
    finally:
        db.close()

def get_station_names():
    """DB에서 측정소 이름 목록을 가져오는 함수"""
    db = DatabaseManager()
    try:
        db.connect()
        query = "SELECT station_name FROM station"
        df = db.execute_query(query)
        return df['station_name'].tolist()
    except Exception as e:
        print(f"측정소 목록 조회 중 오류 발생: {e}")
        return []
    finally:
        db.close()

## 메인 실행
if __name__ == "__main__":
    # API에서 측정소 정보 가져오기
    station_url = 'http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList'
    station_params = {
        'numOfRows': '665',
        'returnType': 'json'
    }
    
    station_df = get_api_data(station_url, station_params)
    
    if station_df is not None:
        # 측정소 정보 저장
        station_query = """
            INSERT INTO station (station_name, addr, dm_x, dm_y, item, mang_name, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_name) DO UPDATE
            SET addr = EXCLUDED.addr,
                dm_x = EXCLUDED.dm_x,
                dm_y = EXCLUDED.dm_y,
                item = EXCLUDED.item,
                mang_name = EXCLUDED.mang_name,
                year = EXCLUDED.year
        """
        
        station_value_mapping = {
            'stationName': 'station_name',
            'addr': 'addr',
            'dmX': 'dm_x',
            'dmY': 'dm_y',
            'item': 'item',
            'mangName': 'mang_name',
            'year': 'year'
        }
        
        save_to_database(station_df, station_query, station_value_mapping)
        
        # DB에서 측정소 이름 목록 가져오기
        station_names = get_station_names()
        
        # 각 측정소별 실시간 데이터 조회
        real_time_url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
        
        for station_name in station_names:
            print(f"측정소 {station_name} 데이터 조회 중...")
            real_time_params = {
                'dataTerm': 'MONTH',
                'stationName': station_name,
                'returnType': 'json'
            }
            
            real_time_df = get_api_data(real_time_url, real_time_params)
            
            if real_time_df is not None:
                # 실시간 데이터 저장 쿼리와 매핑 정보는 실시간 데이터의 구조에 맞게 수정 필요
                real_time_query = """
                    INSERT INTO real_time_data (
                        station_name, data_time, so2_value, co_value, o3_value, 
                        no2_value, pm10_value, pm25_value
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_name, data_time) DO UPDATE
                    SET so2_value = EXCLUDED.so2_value,
                        co_value = EXCLUDED.co_value,
                        o3_value = EXCLUDED.o3_value,
                        no2_value = EXCLUDED.no2_value,
                        pm10_value = EXCLUDED.pm10_value,
                        pm25_value = EXCLUDED.pm25_value
                """
                
                real_time_value_mapping = {
                    'stationName': 'station_name',
                    'dataTime': 'data_time',
                    'so2Value': 'so2_value',
                    'coValue': 'co_value',
                    'o3Value': 'o3_value',
                    'no2Value': 'no2_value',
                    'pm10Value': 'pm10_value',
                    'pm25Value': 'pm25_value'
                }
                
                save_to_database(real_time_df, real_time_query, real_time_value_mapping)

