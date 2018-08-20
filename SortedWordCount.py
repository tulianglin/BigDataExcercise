#from pyspark import SparkContext, SparkConf

#conf = SparkConf().setAppName("wordCounts").setMaster("local[*]")
#sc = SparkContext(conf = conf)
    
lines = sc.textFile("/FileStore/tables/word_count.text")
wordRdd = lines.flatMap(lambda line: line.split(" "))

wordPairRdd = wordRdd.map(lambda word: (word, 1))
wordToCountPairs = wordPairRdd.reduceByKey(lambda x, y: x + y)

sortedWordCountPairs = wordToCountPairs \
        .sortBy(lambda wordCount: wordCount[1], ascending=False)

for word, count in  sortedWordCountPairs.collect():
	print("{} : {}".format(word, count))

