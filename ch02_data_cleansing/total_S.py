# 고객마다 얼만큼의 물건을 구매했는가?
# 라이브러리
from pyspark import SparkConf, SparkContext

# 사용자 정의 함수
def extractCusPrice (line):
    fileds = line.split(',')
    return(int(fileds[0]),float(fileds[2]))



# main 함수
def main():
    # spark setting
    conf = SparkConf().setMaster("local").setAppName('SpentbyCustomer')
    sc = SparkContext(conf = conf)

    # load data
    input = sc.textFile("data/customer-orders.csv")
    # print("is data?")
    # Mapping
    mappedInput = input.map(extractCusPrice)
    totalbyCus = mappedInput.reduceByKey(lambda x , y : x + y )

    # 가격으로 정렬 가격을 key값으로 설정하기 위함.
    filpped = totalbyCus.map(lambda x : (x[1],x[0]))
    totalbyCusSort = filpped.sortByKey()

    results = totalbyCusSort.collect()
    for result in results:
        print(result)



# 실행 코드
if __name__=="__main__":
    main()