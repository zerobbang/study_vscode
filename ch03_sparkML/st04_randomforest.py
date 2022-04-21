from cProfile import label
from pyspark.sql import SparkSession

# 머신러닝 라이브러리
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("RandomForest").getOrCreate()

# load data
data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
print(type(data))

# Feature Engineering
## label column
labelIndexer = StringIndexer(inputCol="label", outputCol = "indexedLabel").fit(data)

# 범주형 데이터 체크, 인덱스화
featureIndexer = VectorIndexer(inputCol = "features", outputCol = "IndexedFeatures",maxCategories=4).fit(data)

# data 분리
(trainData, testData) = data.randomSplit([0.7,0.3])

# model
rf = RandomForestClassifier(labelCol = "indexedLabel" # 종속변수
                                , featuresCol = "IndexedFeatures" # 독립변수
                                , numTrees = 10)


# outputCol - "indexedLabel" -->> original label로 변환
labelConvereter = IndexToString(inputCol="prediction",outputCol="predictedLabel", labels = labelIndexer.labels)


# 파이프 라인 구축
pipeline = Pipeline(stages = [labelIndexer, featureIndexer, rf, labelConvereter])

# 모델 학습
model = pipeline.fit(trainData)

# prediction
predictions = model.transform(testData)

# 행에 표시할 값 추출
predictions.select("predictedLabel", "label","features").show(5)

# 모형 평가
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol = "prediction", metricName = "accuracy"
)

accuracy = evaluator.evaluate(predictions)
print("test error = %f " % (1.0 - accuracy))


spark.stop()