import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRddPractice2 {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))

    //1、读取文件的数据test.txt
    val file = sc.textFile("test.txt")

    //2、一共有多少个小于20岁的人参加考试
    println(file.groupBy { x =>
      val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(2)
    }.filter(_._1.split("_")(2).toInt < 20).count())

    //3、一共有多少个等于20岁的人参加考试
    println(file.groupBy { x =>
      val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(2)
    }.filter(_._1.split("_")(2).toInt == 20).count())

    //4、一共有多少个大于20岁的人参加考试
    println(file.groupBy { x =>
      val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(2)
    }.filter(_._1.split("_")(2).toInt > 20).count())

    //5、一共有多个男生参加考试
    println(file.groupBy { x =>
      val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(3)
    }.filter(_._1.split("_")(2).equals("男")).count())

    //6、一共有多少个女生参加考试
    println(file.groupBy { x =>
      val datas = x.split(" ")
      datas(0) + "_" + datas(1) + "_" + datas(3)
    }.filter(_._1.split("_")(2).equals("女")).count())

    //7、12班有多少人参加考试
    println(file.groupBy { x =>
      val datas = x.split(" ")
      datas(0) + "_" + datas(1)
    }.filter(_._1.split("_")(0).equals("12")).count())

    //8、13班有多少人参加考试
    println(file.groupBy { x =>
      val datas = x.split(" "); datas(0) + "_" + datas(1)
    }.filter(_._1.split("_")(0).equals("13")).count()
    )

    //9、语文科目的平均成绩是多少
    println(file.filter(_.split(" ")(4).equals("chinese"))
      .map(_.split(" ")(5).toFloat).mean()
    )

    //10、数学科目的平均成绩是多少
    println(file.filter(_.split(" ")(4).equals("math"))
      .map(_.split(" ")(5).toFloat).mean())

    //11、英语科目的平均成绩是多少
    println(file.filter(_.split(" ")(4).equals("english"))
      .map(_.split(" ")(5).toFloat).mean())

    //12、单个人平均成绩是多少
    println(file.map(x => {
      val line = x.split(" ");
      (line(0) + "_" + line(1), line(5).toInt)
    }).map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2)).collect().mkString(","))

    //13、12班平均成绩是多少
    println(file.filter(_.split(" ")(0).equals("12")).map(x => {
      val line = x.split(" ");
      (line(0), line(5).toInt)
    }).map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .map(_._2).collect().mkString(","))

    //14、12班男生平均总成绩是多少
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(0).equals("12") && datas(3).equals("男")
    }.map(x => {
      val line = x.split(" ");
      (line(0), line(5).toInt)
    }).map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2)).map(_._2).collect().mkString(","))

    //15、12班女生平均总成绩是多少？
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(0).equals("12") && datas(3).equals("女")
    }.map(x => {
      val line = x.split(" ");
      (line(0), line(5).toInt)
    }).map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .map(_._2).collect().mkString(","))

    //16、13班平均成绩是多少？
    println(file.filter(_.split(" ")(0).equals("13")).map(x => {
      val line = x.split(" ");
      (line(0), line(5).toInt)
    }).map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .map(_._2).collect().mkString(","))

    //17、13班男生平均总成绩是多少
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(0).equals("13") && datas(3).equals("男") }.map(x => {
      val line = x.split(" ");
      (line(0), line(5).toInt)
    }).map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .map(_._2).collect().mkString(","))

    //18、13班女生平均总成绩是多少
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(0).equals("13") && datas(3).equals("女")
    }.map(x => {
      val line = x.split(" ");
      (line(0), line(5).toInt)
    }).map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .map(_._2).collect().mkString(","))

    //19、全校语文成绩最高分是多少
    println(file.filter {
      _.split(" ")(4).equals("chinese")
    }.map(_.split(" ")(5)).max())

    //20、12班语文成绩最低分是多少
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(4).equals("chinese") && datas(0).equals("12")
    }.map(_.split(" ")(5)).min())

    //21、13班数学最高成绩是多少
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(4).equals("math") && datas(0).equals("13")
    }.map(_.split(" ")(5)).max())

    //22、总成绩大于150分的12班的女生有几个
    //方式一：无shuffle
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(3).equals("女") && datas(0).equals("12")
    }.groupBy(_.split(" ")(1)).map {
      case (name, list) => {
        list.map(_.split(" ")(5).toFloat).sum
      }
    }.filter(_ > 150).count())

    //方式二：有shuffle
    println(file.filter { x =>
      var datas = x.split(" ")
      datas(3).equals("女") && datas(0).equals("12")
    }.map { x => var datas = x.split(" ")
      (datas(1), datas(5).toInt)
    }.reduceByKey(_ + _).filter(_._2 > 150).count())

    //23、总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少
    val data1 = file.map(x => {
      val line = x.split(" ");
      (line(0) + "," + line(1) + "," + line(3), line(5).toInt)
    })
    val data2 = file.map(x => {
      val line = x.split(" ");
      (line(0) + "," + line(1) + "," + line(3) + "," + line(4), line(5).toInt)
    })

    //过滤出总分大于150的,并求出平均成绩
    val com1: RDD[(String, Int)] = data1
      .map(a => (a._1, (a._2, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter(x => (x._2._1 > 150))
      .map(t => (t._1, t._2._1 / t._2._2))

    //过滤出 数学大于等于70，且年龄大于等于19岁的学生
    val com2: RDD[(String, Int)] = data2
      .filter(x => {
        val datas = x._1.split(",");
        datas(3).equals("math") && x._2 > 70
      })
      .map(x => {
        val datas = x._1.split(",");
        (datas(0) + "," + datas(1) + "," + datas(2), x._2.toInt)
      })

    println(com1.join(com2).map(x => (x._1, x._2._1)).collect().mkString(","))

    sc.stop()


  }

}
