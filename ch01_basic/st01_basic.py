# -*- coding: utf-8 -*- 

import pyspark
print(pyspark.__version__)

from pyspark.sql import SparkSession

# 스파크 세션 초기화
spark = SparkSession.builder.master('local[1]').appName('SampleTutorial').getOrCreate()
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

print("rdd Count:", rdd.count())

spark.stop()
