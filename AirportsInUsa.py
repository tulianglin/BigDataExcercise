#from pyspark import SparkContext, SparkConf

def splitComma(line: str):
    splits = line.split(',')
    return "{}, {}".format(splits[1], splits[2])

#conf = SparkConf().setAppName("airports").setMaster("local[*]")
#sc = SparkContext(conf = conf)

airports = sc.textFile("/FileStore/tables/airports.text")
airportsInUSA = airports.filter(lambda line : line.split(',')[3] == "\"United States\"")

airportsNameAndCityNames = airportsInUSA.map(splitComma)
airportsNameAndCityNames.saveAsTextFile("/FileStore/airports_in_usa.text")
