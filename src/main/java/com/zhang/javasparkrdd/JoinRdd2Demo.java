package com.zhang.javasparkrdd;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class JoinRdd2Demo {

    public static void main(String[] args) {
        SparkConf conf= new SparkConf().setAppName("JoinRdd1Demo").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        final Random random = new Random();
        JavaRDD<Integer> javaRdd = sc.parallelize(data);
        JavaPairRDD<Integer, Integer> javaPairRDD = javaRdd.mapToPair((PairFunction<Integer, Integer, Integer>) num ->
                new Tuple2<>(num, random.nextInt(10))
        );

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinRDD = javaPairRDD.join(javaPairRDD);
        System.out.println(joinRDD.collect());

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinRDD2 = javaPairRDD.join(javaPairRDD,2);
        System.out.println(joinRDD2.collect());

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinRDD3 = javaPairRDD.join(javaPairRDD, new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }
            @Override
            public int getPartition(Object key) {
                return (key.toString()).hashCode()%numPartitions();
            }
        });
        System.out.println(joinRDD3.collect());
    }
}
