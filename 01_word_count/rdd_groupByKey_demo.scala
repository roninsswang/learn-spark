// 使用word count案例，将相同的单词收集到一起

import org.apache.spark.rdd.RDD

// 以行为单位做分词
val cleanWordRDD: RDD[String] =  wordRDD.filter(word => !word.equals(""))

//把普通RDD映射成Paired RDD
val kvRDD: RDD[(String, String)] = cleanWordRDD.map(word => (word, word))

//按照单词做分组收集
val words: RDD[(String, Iterable[String])] = kvRDD.groupByKey()