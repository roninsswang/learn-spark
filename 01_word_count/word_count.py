from pyspark import SparkContext

text_file = SparkContext().textFile("/Users/roninsswang/codehub/ronincode/learn-spark/01_word_count/wikiOfSpark.txt")

word_count = (
    text_file.flatMap(lambda line: line.split(" "))   # 以行为单位进行分词
    .filter(lambda word: word != "")                  # 去掉空字符串
    .map(lambda word: (word, 1))                      # 把元素转成(key,value)形式
    .reduceByKey(lambda x, y: x + y)                  # 分组计数
    .sortBy(lambda x: x[1], False)                    # 按词频降序排序
    .take(5)                                          # 取前五个
)
print(word_count)