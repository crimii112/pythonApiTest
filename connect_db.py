from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd

class DatabaseManager:
    load_dotenv()
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWD')
    
    def __init__(self, host=host, port=port, database=database, user=user, password=password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        """데이터베이스 연결"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("데이터베이스 연결 성공")
        except Exception as e:
            print(f"데이터베이스 연결 실패: {e}")
            raise

    def execute_query(self, query):
        """SQL 쿼리 실행 및 결과 반환 (SELECT 쿼리용)"""
        try:
            df = pd.read_sql(query, con=self.connection)
            return df
        except Exception as e:
            print(f"쿼리 실행 실패: {e}")
            raise

    def execute_insert(self, query, values):
        """데이터 삽입 쿼리 실행 (INSERT 쿼리용)"""
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except Exception as e:
            print(f"데이터 삽입 실패: {e}")
            self.connection.rollback()
            raise

    def execute_batch_insert(self, query, values_list):
        """여러 데이터 한번에 삽입 (배치 INSERT용)"""
        try:
            self.cursor.executemany(query, values_list)
            self.connection.commit()
        except Exception as e:
            print(f"배치 데이터 삽입 실패: {e}")
            self.connection.rollback()
            raise

    def close(self):
        """데이터베이스 연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("데이터베이스 연결 종료")
