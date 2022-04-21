from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F


# 스파크 세션 생성
spark = SparkSession.builder.master("local[1]").appName("MLSampleTuto").getOrCreate()

# load data
# 압축 해제 여기에서 가능
df = spark.read.csv("data/AA_DFW_2014_Departures_Short.csv.gz", header = True)
print("file loaded")

# remove duration = 0
df = df.filter(df[3] > 0) 

# Add ID col 
df = df.withColumn('id', F.monotonically_increasing_id())

# file 내보내기
df.write.csv("data/output.csv", mode = "overwrite")

df.show()
spark.stop()