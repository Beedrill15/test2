from pyspark import SparkContext, SparkConf

conf = Sparkconf().setAppname('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)

text_file = sc.textFile("shakespeare.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("PythonOut.txt")
