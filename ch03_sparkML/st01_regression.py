from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

print("start")

# setting
spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

# load data
# StructType 생량
data = spark.read.option("header","true").option("inferSchema","true").csv("data/realestate.csv")

# dataframe -> 행렬
assembler = VectorAssembler().setInputCols(['HouseAge','DistanceToMRT','NumberConvenienceStores']).setOutputCol("features")

# Target data setting
df = assembler.transform(data).select("PriceOfUnitArea","features")

# separate data
trainTest = df.randomSplit([0.5,0.5])
trainDF = trainTest[0]
testDF = trainTest[1]

# Decision Tree class define
dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")

# model learning
model = dtr.fit(trainDF)
model

# predict model
fullPredictions = model.transform(testDF).cache()

# 예측값과 Label 분리
predictions = fullPredictions.select("prediction").rdd.map(lambda x : x[0])

# real data
labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x : x[0])

# zip
preds_label = predictions.zip(labels).collect()

for prediction in preds_label:
    print(prediction)


# session stop
spark.stop()

