package com.zhang.javasparkrdd;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CogroupRddDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark WordCount!").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> namesList = Arrays.asList(
                new Tuple2<Integer, String>(1, "Spark"),
                new Tuple2<Integer, String>(3, "Tachyon"),
                new Tuple2<Integer, String>(4, "Sqoop"),
                new Tuple2<Integer, String>(2, "Hadoop"),
                new Tuple2<Integer, String>(2, "Hadoop2")
        );

        List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(3, 70),
                new Tuple2<Integer, Integer>(3, 77),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(2, 80)
        );
        JavaPairRDD<Integer, String> names = sc.parallelizePairs(namesList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoresList);
        /**
         * <Integer> JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>
         * org.apache.spark.api.java.JavaPairRDD.cogroup(JavaPairRDD<Integer, Integer> other)
         */
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> nameScores = names.cogroup(scores);

        nameScores.foreach((VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>) t -> {
            int i = 1;
            String string = "ID:" + t._1 + " , " + "Name:" + t._2._1 + " , " + "Score:" + t._2._2;
            string += "     count:" + i;
            System.out.println(string);
            i++;
        });

        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = sc.parallelize(data);

        JavaPairRDD<Integer, Integer> javaPairRDD = javaRDD.mapToPair((PairFunction<Integer, Integer, Integer>) t ->
                new Tuple2<>(t, 1)
        );

        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupRDD = javaPairRDD.cogroup(javaPairRDD);
        System.out.println(cogroupRDD.collect());

        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupRDD3 = javaPairRDD.cogroup(javaPairRDD, new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object key) {
                return (key.toString()).hashCode() % numPartitions();
            }
        });

        System.out.println(cogroupRDD3);

        sc.close();
    }

}
