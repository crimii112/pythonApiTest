from dotenv import load_dotenv
import os
import requests
from openapiTest.db_manager import DatabaseManager
import pandas as pd
import time

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

def save_to_database(df, query_template, value_mapping, station_name=None):
    """
    DataFrame을 DB에 저장하는 함수
    
    Args:
        df (DataFrame): 저장할 데이터가 있는 DataFrame
        query_template (str): INSERT 쿼리 템플릿
        value_mapping (dict): DataFrame 컬럼명과 DB 컬럼명의 매핑
        station_name (str, optional): 저장할 측정소 이름
    """
    db = DatabaseManager()
    
    try:
        # DB 연결
        db.connect()
        
        # DataFrame의 각 행을 DB에 저장
        for _, row in df.iterrows():
            # value_mapping을 기반으로 실제 값 생성
            values = []
            for df_col in value_mapping.keys():
                value = row[df_col]
                # 측정값 컬럼인 경우 숫자로 변환
                if df_col in ['so2Value', 'coValue', 'pm10Value', 'o3Value', 'no2Value']:
                    try:
                        # '-' 또는 빈 문자열인 경우 None으로 처리
                        value = None if value in ['-', ''] else float(value)
                    except (ValueError, TypeError):
                        value = None
                values.append(value)
            
            values = tuple(values)
            
            # station_name이 제공된 경우, values 튜플의 시작에 추가
            if station_name is not None:
                values = (station_name,) + values
            
            print(values)
            
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
    # station_url = 'http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList'
    # station_params = {
    #     'numOfRows': '665',
    #     'returnType': 'json'
    # }
    
    # station_df = get_api_data(station_url, station_params)
    
    # if station_df is not None:
    #     # 측정소 정보 저장
    #     station_query = """
    #         INSERT INTO station (station_name, addr, dm_x, dm_y, item, mang_name, year)
    #         VALUES (%s, %s, %s, %s, %s, %s, %s)
    #         ON CONFLICT (station_name) 
    #         DO UPDATE SET addr = EXCLUDED.addr,
    #                         dm_x = EXCLUDED.dm_x,
    #                         dm_y = EXCLUDED.dm_y,
    #                         item = EXCLUDED.item,
    #                         mang_name = EXCLUDED.mang_name,
    #                         year = EXCLUDED.year
    #     """
        
    #     station_value_mapping = {
    #         'stationName': 'station_name',
    #         'addr': 'addr',
    #         'dmX': 'dm_x',
    #         'dmY': 'dm_y',
    #         'item': 'item',
    #         'mangName': 'mang_name',
    #         'year': 'year'
    #     }
        
    #     save_to_database(station_df, station_query, station_value_mapping)
        
    # DB에서 측정소 이름 목록 가져오기
    station_names = get_station_names()
    
    # 처음 400개의 측정소 사용 => api 호출 일일 트래픽 제한 때문에 나눠서 저장 => 날짜 하루 밀리는 것 고려해야 함
    # station_names = station_names[:400]
    # 400번째 이후의 측정소 사용
    # station_names = station_names[400:]
    
    print(f"처리할 측정소 수: {len(station_names)}")
    
    # 각 측정소별 실시간 데이터 조회
    real_time_url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty'
    
    for i, station_name in enumerate(station_names, 1):
        print(f"[{i}/{len(station_names)}] 측정소 {station_name} 데이터 조회 중...")
        
        real_time_params = {
            'dataTerm': 'MONTH',
            'stationName': station_name,
            'returnType': 'json',
            'numOfRows': '744'
        }
        
        real_time_df = get_api_data(real_time_url, real_time_params)
        
        if real_time_df is not None:
            real_time_query = """
                INSERT INTO data_by_station (
                    station_name, data_time, so2_value, co_value, o3_value, no2_value, pm10_value
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_name, data_time) 
                DO UPDATE SET so2_value = EXCLUDED.so2_value,
                                co_value = EXCLUDED.co_value,
                                o3_value = EXCLUDED.o3_value,
                                no2_value = EXCLUDED.no2_value,
                                pm10_value = EXCLUDED.pm10_value
            """
            
            real_time_value_mapping = {
                'dataTime': 'data_time',
                'so2Value': 'so2_value',
                'coValue': 'co_value',
                'o3Value': 'o3_value',
                'no2Value': 'no2_value',
                'pm10Value': 'pm10_value'
            }
            
            save_to_database(real_time_df, real_time_query, real_time_value_mapping, station_name)
            print(f"측정소 {station_name} 데이터 저장 완료")
        else:
            print(f"측정소 {station_name} 데이터 조회 실패")
        
        # API 호출 제한을 고려하여 잠시 대기
        time.sleep(0.1)  # 0.1초 대기

