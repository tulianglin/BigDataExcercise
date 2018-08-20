#from pyspark import SparkContext, SparkConf

#conf = SparkConf().setAppName("wordCounts").setMaster("local[3]")
#sc = SparkContext(conf = conf)

lines = sc.textFile("/FileStore/tables/word_count.text")
wordRdd = lines.flatMap(lambda line: line.split(" "))
wordPairRdd = wordRdd.map(lambda word: (word, 1))

wordCounts = wordPairRdd.reduceByKey(lambda x, y: x + y)
for word, count in wordCounts.collect():
	print("{} : {}".format(word, count))
