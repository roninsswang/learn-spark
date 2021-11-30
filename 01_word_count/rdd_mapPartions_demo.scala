// demo 修改word count的计数需求，从原来的对单词的计数，改为对单词的哈希值计数
//把普通的RDD转换称为Paired RDD
import java.security.MessageDigest

val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

val kvRDD: RDD[(String, Int)] = cleanWordRDD.mapPartitions{ partition => 
    // 此处是以数据分区为粒度来获取MD5对象实例
    val md5 = MessageDigest.getInstance("MD5")
    val newPartition = partition.map(word => {
        // 在处理每一条数据记录的时候，可以复用同一个Partition内的MD5对象
        (md5.digest(word.getBytes).mkString, 1)
    })
    newPartition
}