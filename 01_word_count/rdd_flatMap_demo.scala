//读取文件内容
val lineRDD: RDD[String] = spark.sparkContext.textFile(file)

//以行为单位提取相邻单词
val wordPairedRDD: RDD[String] = lineRDD.flatMap(line => {
    //将行转换为单词数组
    val words： Array[String] = line.split(" ")
    //将单个单词数组转换成相邻单词的数组
    for （i <- 0 until words.length - 1） yield words(i) + "-" + words(i + 1)
})