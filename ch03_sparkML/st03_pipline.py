from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PipeLine").getOrCreate()

# 가상의 데이터 생성
training = spark.createDataFrame(
    [
        (0, "a b c d e spark", 1.0),
        (1, "b d", 0.0),
        (2, "spark f g h", 1.0),
        (3, "hadoop mapreduce", 0.0)
    ]
    , ["id", "text", "label"])

training.show()


# text data split
## Feature Engineering

# text -> words
tokenizer = Tokenizer(inputCol = "text", outputCol = "words")

# words -> number
hashingTF = HashingTF(inputCol = tokenizer.getOutputCol(), outputCol = "features")

# model 가져오기
lr = LogisticRegression(maxIter =5, regParam=0.01)

# pipeline 구축
pipeline = Pipeline(stages = [tokenizer, hashingTF, lr])

# pipeline 실행
model = pipeline.fit(training)

# text data 생성
# Prepare test data, which are unlabeled (id, text) tuples.
test = spark.createDataFrame(
    [
        (4, "spark i j k"),
        (5, "l m n"),
        (6, "spark hadoop spark"),
        (7, "apache hadoop")
    ]
    , ["id", "text"])

# prediction
prediction = model.transform(test)
selected = prediction.select("id","text","probability","prediction")

for row in selected.collect():
    row_id, text, prob, prediction = row
    print(
        # 문자열 포맷팅
        "(%d, %s) -------->  probability = %s, prediction = %f" % (row_id, text, str(prob), prediction)
    )

spark.stop()


# result
# spark O = prediction 1
# spark X = prediction 0