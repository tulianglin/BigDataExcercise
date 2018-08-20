#from pyspark import SparkContext, SparkConf

#conf = SparkConf().setAppName("primeNumbers").setMaster("local[*]")
#sc = SparkContext(conf = conf)
    
lines = sc.textFile("/FileStore/tables/prime_nums.text")
numbers = lines.flatMap(lambda line: line.split("\t"))

validNumbers = numbers.filter(lambda number: number)
    
intNumbers = validNumbers.map(lambda number: int(number))
    
print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))

