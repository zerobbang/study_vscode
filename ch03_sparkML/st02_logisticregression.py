# ML with pyspark
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.appName("logistic").getOrCreate()

# load data
training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

# 모델 만들기
mlr = LogisticRegression()
mlr_model = mlr.fit(training)

# 로지스틱 회귀, 선형 모델 -> 기울기와 절편 추출
print("Coefficients" + str(mlr_model.coefficients))
print("Intercept" + str(mlr_model.intercept))


spark.stop()