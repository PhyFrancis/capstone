package com.capstone.maven.mostpopularairports;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class SSMostPopularAirports {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Flight Count"));

		JavaRDD<String> tokenized = sc.textFile(args[0]).flatMap(new FlatMapFunction<String, String>() {
			@SuppressWarnings("unchecked")
			public Iterator<String> call(String s) {
				return (Iterator<String>) Arrays.asList(s.split(" "));
			}
			private static final long serialVersionUID = 2001L;
		});
		
		// count the occurrence of each word
	    JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s, 1);
	        }
	        private static final long serialVersionUID = 2002L;
	      }
	    ).reduceByKey(
	      new Function2<Integer, Integer, Integer>() {
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	        private static final long serialVersionUID = 2003L;
	      }
	    );
	}
}
