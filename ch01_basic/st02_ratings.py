# SparkContext로 raw data --> RDD 변환

from pyspark import SparkConf, SparkContext
import collections

print("H")

def main():
    # MasterNode = local
    # MapReduce

    conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
    sc = SparkContext(conf = conf)

    # 데이터 불러오고 저장
    lines = sc.textFile("ml-100k/u.logs")
    
    # 데이터 쪼개기
    # 3번째 데이터 열의 값들만 분리하여 ratings 변수에 저장
    ratings = lines.map(lambda x:  x.split()[2])

    print("ratings: ",ratings)

    # 값 세기
    result = ratings.countByValue()
    print("result:", result)

    # 정렬
    sortedResults = collections.OrderedDict(sorted(result.items()))
    for key, value in sortedResults.items():
        print("%s %i" % (key,value))

if __name__=="__main__":
    main()