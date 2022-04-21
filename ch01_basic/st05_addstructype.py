# add moive title
# use u.item data file
from pyspark.sql import SparkSession
from pyspark .sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
# codecs -> 파일 압축 또는 해제
import codecs

# load title
def loadMovieNames():
    # dictionary 형태
    MovieNames = {}
    # search 'import codecs encoding' or 'File encoding windows'
    # windows -> ISO-8859-1
    with codecs.open("ml-100k/u.item","r", encoding="ISO-8859-1", errors = "igonre") as f:
        for line in f:
            # 자료가 |로 구분 되어 있다. -- | 로 split
            fields = line.split("|")
            # movie title을 'MovieNames'의 첫번째 열에 숫자 형태로 저장
            MovieNames[int(fields[0])] = fields[1]
    return MovieNames


# session 할당
spark = SparkSession.builder.appName("PopularMovie").getOrCreate()

# 서로 다른 크기의 데이터 합치기 ->broadcast
#  파이썬 딕셔너리를 스파크 객체로 반환
nameDict = spark.sparkContext.broadcast(loadMovieNames())


# 스키마 작성 StructType -- u.logs의 데이터
schema = StructType(
    [
        StructField("userID", IntegerType(), True) # True 결측치 허용
        , StructField("movieID", IntegerType(), True)
        , StructField("rating", IntegerType(), True)
        , StructField("timestamp", LongType(), True)
    ]
)
print("Schema is done..!")


# load Data
# 공백 = \t
movies_df = spark.read.option("sep", "\t").schema(schema).csv("ml-100k/u.logs")
movies_df.show(10)

# 영화별로 묶는다.
# movieID -> group / count 
topMovieIDs = movies_df.groupBy("movieID").count()
# topMovieIDs = movies_df.groupBy("movieID").count().orderBy(func.desc("count"))


# dict : key -vlaue 가져오기
# 즉 키 값을 알면 value 자동으로 가져온다. value = movieTitle
def lookupName(movieID):
    return nameDict.value[movieID]

# 사용자 정의 함수 사용시 udf 명시해주기
lookupNameUDF = func.udf(lookupName)


# Add movie title to topMovieIDs
# withColumn = add col
moviesWithNames = topMovieIDs.withColumn("movieTitle",lookupNameUDF(func.col("movieID")))
final_df = moviesWithNames.orderBy(func.desc("count"))

final_df.show(10)


# session stop
spark.stop()