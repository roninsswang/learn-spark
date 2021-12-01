//使用SparkContext.parallelize在内部数据创建

import org.apache.spark.rdd.RDD

val words: Array[String] = Array("Spark", "is", "cool")
val rdd: RDD[String] sc.parallelize(words)



// 使用SparkContext.textFile从外部数据创建

import org.apache.spark.rdd.RDD
val file: String = "./wikiOfSpark.txt"
//读取文件内容
val lineRDD: RDD[String] = spark.sparkContext.textFile(file)


