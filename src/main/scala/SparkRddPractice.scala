import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

class SparkRddPractice {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkRddPractice").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //1、创建一个1-10数组的RDD，将所有元素*2形成新的RDD
    val rdd1 = sc.makeRDD(1 to 10).map(_ * 2)

    //2、创建一个10-20数组的RDD，使用mapPartitions将所有元素*2形成新的RDD
    val rdd2 = sc.makeRDD(10 to 20).mapPartitions(_.map(_ * 2))

    //3、创建一个元素为 1-5 的RDD，运用 flatMap创建一个新的 RDD，新的 RDD 为原 RDD 每个元素的 平方和三次方 来组成 1,1,4,8,9,27..
    val rdd3 = sc.makeRDD(1 to 5).flatMap(x => Array(math.pow(x, 2), math.pow(x, 3)))

    //4、创建一个 4 个分区的 RDD数据为Array(10,20,30,40,50,60)，使用glom将每个分区的数据放到一个数组
    val rdd4 = sc.makeRDD(Array(10, 20, 30, 40, 50, 60)).glom()

    //5、创建一个 RDD数据为Array(1, 3, 4, 20, 4, 5, 8)，按照元素的奇偶性进行分组
    val rdd5 = sc.makeRDD(Array(1, 3, 4, 20, 4, 5, 8)).groupBy(x => if (x % 2 == 0) "偶数" else "奇数")

    //6、创建一个 RDD（由字符串组成）Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong")，过滤出一个新 RDD（包含“xiao”子串）
    val rdd6 = sc.makeRDD(Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong")).filter(x => x.contains("xiao"))

    //7、创建一个 RDD数据为1 to 10，请使用sample不放回抽样
    val rdd7 = sc.makeRDD(1 to 10).sample(false, 0.5, 1)

    //8、创建一个 RDD数据为1 to 10，请使用sample放回抽样
    val rdd8 = sc.makeRDD(1 to 10).sample(true, 0.5, 1)

    //9、创建一个 RDD数据为Array(10,10,2,5,3,5,3,6,9,1),对 RDD 中元素执行去重操作
    val rdd9 = sc.makeRDD(Array(10, 10, 2, 5, 3, 5, 3, 6, 9, 1)).distinct()

    //10、创建一个分区数为5的 RDD，数据为0 to 100，之后使用coalesce再重新减少分区的数量至 2
    val rdd10 = sc.makeRDD(0 to 100, 5).coalesce(2)

    //11、创建一个分区数为5的 RDD，数据为0 to 100，之后使用repartition再重新减少分区的数量至 3
    val rdd11 = sc.makeRDD(0 to 100, 5).repartition(3)

    //12、创建一个 RDD数据为1,3,4,10,4,6,9,20,30,16,请给RDD进行分别进行升序和降序排列
    val rdd12 = sc.makeRDD(Array(1, 3, 4, 10, 4, 6, 9, 20, 30, 16))
    val rdd12Asc = rdd12.sortBy(x => x)
    val rdd12Desc = rdd12.sortBy(x => x, false)

    //13、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，求并集
    val rdd13_1 = sc.makeRDD(1 to 6)
    val rdd13_2 = sc.makeRDD(4 to 10)

    val rdd13 = rdd13_1.union(rdd13_2)

    //14、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算差集，两个都算
    val rdd14_1 = rdd13_1.subtract(rdd13_2)
    val rdd14_2 = rdd13_2.subtract(rdd13_1)

    //15、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算交集
    val rdd15 = rdd13_1.intersection(rdd13_2)

    //16、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算 2 个 RDD 的笛卡尔积
    val rdd16 = rdd13_1.cartesian(rdd13_2)

    //17、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 5和11 to 15，对两个RDD拉链操作
    val rdd17_1 = sc.makeRDD(1 to 5)
    val rdd17_2 = sc.makeRDD(11 to 15)
    val rdd17 = rdd17_1.zip(rdd17_2)

    //18、创建一个RDD数据为List(("female",1),("male",5),("female",5),("male",2))，请计算出female和male的总数分别为多少
    val rdd18 = sc.makeRDD(List(("female", 1), ("male", 5), ("female", 5), ("male", 2))).reduceByKey(_ + _)

    //19、创建一个有两个分区的 RDD数据为List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))，取出每个分区相同key对应值的最大值，然后相加
    val rdd19 = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    rdd19.glom().collect().foreach(x => println(x.mkString(",")))
    val data19Result = rdd19.aggregateByKey(0)(math.max(_, _), _ + _)

    //20、 创建一个有两个分区的 pairRDD数据为Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))，根据 key 计算每种 key 的value的平均值
    val rdd20 = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
      .groupByKey().map(x => x._1 -> x._2.sum / x._2.size)
    //or
    val rdd20or = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
      .map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._1))

    /*
    21、统计出每一个省份广告被点击次数的 TOP3，数据在access.log文件中
    数据结构：时间戳，省份，城市，用户，广告 字段使用空格分割。
    样本如下：
    1516609143867 6 7 64 16
    1516609143869 9 4 75 18
    1516609143869 1 7 87 12
    */
    val file1 = sc.textFile("input/access.log")
    file1.map { x =>
      val data = x.split(" ")
      (data(1), (data(4), 1))
    }.groupByKey().map {
      case (province, list) =>
        val data = list.groupBy(_._1)
          .map(x => (x._1, x._2.size))
          .toList.sortWith((x, y) => x._2 > y._2)
          .take(3)
        (province, data)
    }.collect().sortBy(_._1).foreach(println)


    //22、读取本地文件words.txt,统计出每个单词的个数，保存数据到 hdfs 上
    val file2 = sc.textFile("input/words.txt")
      .flatMap(x => x.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("hdfs://mycluster:8020/20200407_wordsOutput")

    //23、读取 people.json 数据的文件, 每行是一个 json 对象，进行解析输出
    val rdd23 = sc.textFile("input/people.json").map(JSON.parseFull)

    //24、保存一个 SequenceFile 文件，使用spark创建一个RDD数据为Array(("a", 1),("b", 2),("c", 3))，保存为SequenceFile格式的文件到hdfs上
    val rdd24 = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
      .saveAsSequenceFile("hdfs://mycluster:8020/20200407_SequenceFile")

    //25、读取24题的SequenceFile 文件并输出
    val rdd25 = sc.sequenceFile("hdfs://mycluster:8020/20200407_SequenceFile")
    //26、读写 objectFile 文件，把 RDD 保存为objectFile，RDD数据为Array(("a", 1),("b", 2),("c", 3))，并进行读取出来

    //27、使用内置累加器计算Accumulator.txt文件中空行的数量

    /*
    28、使用Spark广播变量
    用户表：
    id name age gender(0|1)
    001,刘向前,18,0
    002,冯  剑,28,1
    003,李志杰,38,0
    004,郭  鹏,48,2
    要求，输出用户信息，gender必须为男或者女，不能为0,1
    使用广播变量把Map("0" -> "女", "1" -> "男")设置为广播变量，最终输出格式为
    001,刘向前,18,女
    003,李志杰,38,女
    002,冯  剑,28,男
    004,郭  鹏,48,男
    */

    /*
    29、mysql创建一个数据库bigdata0407，在此数据库中创建一张表
    CREATE TABLE `user` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `username` varchar(32) NOT NULL COMMENT '用户名称',
    `birthday` date DEFAULT NULL COMMENT '生日',
    `sex` char(1) DEFAULT NULL COMMENT '性别',
    `address` varchar(256) DEFAULT NULL COMMENT '地址',
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
    数据依次是：姓名 生日 性别 省份
      请使用spark将以上数据写入mysql中，并读取出来
      */

    /*
    30、在hbase中创建一个表student，有一个 message列族
      create 'student', 'message'
    scan 'student', {COLUMNS => 'message'}
    给出以下数据，请使用spark将数据写入到hbase中的student表中,并进行查询出来
    数据如下：
    依次是：姓名 班级 性别 省份，对应表中的字段依次是：name,class,sex,province
    */


  }

}
