// 把普通的RDD转成（Key, Value）的RDD,也称为Paired RDD
val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
val  kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1)   // =>左边是输入形参，右边是输出结果


// 把RDD元素转换为（Key, Value)的形式
//定义映射函数f
def f(word: String): (String, Int) = {
return (word, 1)
}

val] kvRDD: RDD[(String, Int)] = cleanWordRDD.map(f)


//demo 修改word count 的计数逻辑，把“Spark”这个单词的统计计数权重提高一倍
//定义映射函数f
def f(word, String): (Stirng, Int) = {
if (word.equals("Spark")) {return (word, 2)}
return (word, 1)
}

val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(f)



// demo 修改word count的计数需求，从原来的对单词的计数，改为对单词的哈希值计数
//把普通的RDD转换称为Paired RDD
import java.security.MessageDigest

val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

val kvRDD: RDD[(String, Int)] = cleanWordRDD.map{ word => 
    //获取MD5对象
    val md5 = MessageDigest.getInstance("MD5")
    //使用MD5计算hash值
    val hash = md5.digest(word.getBytes).mkString
    //返回哈希值与数字1的Pair
    (hash, 1)
}
// 上述这种demo，反映了map(f)的性能问题，在处理每条记录都需要事先创建MessageDigest,执行效率大大降低


















































