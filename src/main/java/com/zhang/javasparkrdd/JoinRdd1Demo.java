package com.zhang.javasparkrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JoinRdd1Demo {

    public static void main(String[] args) {

        SparkConf conf= new SparkConf().setAppName("JoinRdd1Demo").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> rdd = sc.parallelize(data);

        JavaPairRDD<Integer, Integer> firstRdd = rdd.mapToPair((PairFunction<Integer, Integer, Integer>) num ->
                new Tuple2<>(num, num * num)
        );

        JavaPairRDD<Integer, String> secondRdd = rdd.mapToPair((PairFunction<Integer, Integer, String>) num ->
           new Tuple2<>(num, String.valueOf((char)(64 + num * num)))
        );

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinRdd = firstRdd.join(secondRdd);

        JavaRDD<String> res = joinRdd.map((Function<Tuple2<Integer, Tuple2<Integer, String>>, String>) integerTuple2Tuple2 -> {
            int key = integerTuple2Tuple2._1();
            int value1 = integerTuple2Tuple2._2()._1();
            String value2 = integerTuple2Tuple2._2()._2();
            return "<" + key + ",<" + value1 + "," + value2 + ">>";
        });

        List<String> resList = res.collect();
        for(String str : resList) {
            System.out.println(str);
        }

        sc.stop();

    }

}
