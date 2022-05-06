# Spark sql -> data 추출

# 라이브러리
from pyspark.sql import SparkSession
import pandas as pd

"""
# session setting
my_spark = SparkSession.builder.getOrCreate()
print(my_spark)

# 테이블을 확인하는 코드
print(my_spark.catalog.listDatabases())
# [Database(name='default', description='default database', locationUri='file:/C:/pys_pro/ch01_start/spark-warehouse')]
# Database로 불러옴.


# point! spark 에서도 sql 가능!
# show database
my_spark.sql('show databases').show()

# 현재 db 확인
my_spark.catalog.currentDatabase() # -> default 나옴.
my_spark.stop()
"""

# --------------------------------------------------------------------------------
# csv 파일 불러오기
spark = SparkSession.builder.master('local[1]').appName("DBTuto").getOrCreate()
flights = spark.read.option('header','true').csv('data/flight_small.csv')
flights.show(4)

# spark.catalog.currentDatabase()
# flights 테이블을 default DB에 추가
flights.createOrReplaceTempView('flights')

# print(spark.catalog.listTables('default'))
# 결과 : [Table(name='flights', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]

# spark.sql('show tables from default').show()



# 쿼리 -> 데이터 저장
query = "From flights SELECT * LIMIT 10"

# 스파크에 세션 할당
flights10 = spark.sql(query)
# flights10.show()

# spark DataFrame  -->  pandas data로 변환
pd.flights10 = flights10.toPandas()
print(pd.flights10.head())