
// 读取文件内容
val lineRDD: RDD[String] = _ // 请参考第一讲获取完整代码
// 以行为单位提取相邻单词
val wordPairRDD: RDD[String] = lineRDD.flatMap( line => {
  // 将行转换为单词数组
  val words: Array[String] = line.split(" ")
  // 将单个单词数组，转换为相邻单词数组
  for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i+1)
})

// 在上面 flatMap 例子的最后，得到了元素为相邻词汇对的 wordPairRDD，它包含的是像“Spark-is”、“is-cool”这样的字符串。
//为了仅保留有意义的词对元素，希望结合标点符号列表，对 wordPairRDD 进行过滤。
//例如，希望过滤掉像“Spark-&”、“|-data”这样的词对。


//定义特殊字符
val list: List[String] = List("&", "|", "#", "^","@")

//定义判断函数f
def f(s: String): Boolean = {
    val words: Array[String] = s.split("_")
    val b1: Boolean = list.contains(words(0)）
    val b2: Boolean = list.contains(words(1))
    return !b1 && !b2  // 返回不在特殊字符列表中的词汇对
}

// 使用filter(f)对RDD进行过滤
val cleanPairRDD: RDD[String] = wordPairedRDD.filter(f)