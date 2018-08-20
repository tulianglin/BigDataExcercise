from pyspark import SparkContext, SparkConf

#conf = SparkConf().setAppName("word count").setMaster("local[*]")
#sc = SparkContext(conf = conf)
    
lines = sc.textFile("/FileStore/tables/word_count.text")
    
words = lines.flatMap(lambda line: line.split(" "))
    
wordCounts = words.countByValue()
    
for word, count in wordCounts.items():
	print("{} : {}".format(word, count))

