package com.capstone.maven.mostpopularairports;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * For question 1 in Group 1.
 */
public class MostPopularAirports {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("topNCount", "10"); // Only get top 10 popular airports
		Job job = Job.getInstance(conf, "most popular airports");
		job.setJarByClass(MostPopularAirports.class);

		// Input
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// Map & Reduce
		job.setMapperClass(AirportCountingMapper.class);
		job.setReducerClass(IntSumReducer.class);

		// Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
