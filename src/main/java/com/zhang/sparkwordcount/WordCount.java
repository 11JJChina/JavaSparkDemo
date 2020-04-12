package com.zhang.sparkwordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {
        String hdfsHost = args[0];
        String hdfsPort = args[1];
        String textFileName = args[2];

        SparkConf conf = new SparkConf().setAppName("WordCountDemo");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String hdfsBasePath = "hdfs://" + hdfsHost + ":" + hdfsPort;
        //文本文件的hdfs路径
        String inputPath = hdfsBasePath + "/input/" + textFileName;

        //输出结果文件的hdfs路径
        String outputPath = hdfsBasePath + "/output/"
                + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        JavaRDD<String> textFile = sc.textFile(inputPath);

        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer,String> sorts = counts
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
                .sortByKey(false);

        List<Tuple2<Integer,String>> top10 = sorts.take(10);

        for(Tuple2<Integer,String> tuple2:top10){
            System.out.println(tuple2._2()+"\t"+tuple2._1());

        }

        sc.parallelize(top10).coalesce(1).saveAsTextFile(outputPath);

        sc.close();

    }

}
