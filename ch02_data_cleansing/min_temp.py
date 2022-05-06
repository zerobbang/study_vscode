from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MinTemperatures')
sc = SparkContext(conf = conf)

print('Begins ... ')

# 줄 쪼개는 함수 만들기
def parseLine (line):
    fileds = line.split(',') 
    # 문자열 split -> list
    stationID = fileds[0]
    entryType = fileds[2]
    # 섭씨로 변환
    temperature = float(fileds[3]) * 0.1 * (9.0 / 5.0 ) + 32.0

    # tuple 형태
    return (stationID, entryType, temperature)

lines = sc.textFile('data/1800.csv')

# print(lines)
# 결과 : data/1800.csv MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0
# 즉, csv 파일을 RDD 형태로 반환

parseLines = lines.map(parseLine)
# print(parseLine)
# 결과 : <function parseLine at 0x0000019A0E84E9E0>
# function으로 들어갔다. -> 메서드 사용 가능

# parseLine x[1]
minTemps = parseLines.filter(lambda x : "TMIN" in x[1])
stationTemps = minTemps.map(lambda x : (x[0],x[2]))
minTemps = stationTemps.map(lambda x ,y : min(x,y))
# 처리 다 했으면 합쳐라
results = minTemps.collect()

print(results)

for results in results:
    print(results[0] + " \t{:.2f}F".format(result[1]))
