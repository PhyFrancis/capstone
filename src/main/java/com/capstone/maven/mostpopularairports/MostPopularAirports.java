package com.capstone.maven.mostpopularairports;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
		Job summarizeJob = Job.getInstance(conf, "most popular airports");
		summarizeJob.setJarByClass(MostPopularAirports.class);
		final String tmp_folder = "/most_popular_airports_tmp";

		// Input
		FileInputFormat.setInputPaths(summarizeJob, new Path(args[0]));
		// Map & Reduce
		summarizeJob.setMapperClass(AirportCountingMapper.class);
		summarizeJob.setReducerClass(IntSumReducer.class);
		// Output
		summarizeJob.setOutputKeyClass(IntWritable.class);
		summarizeJob.setOutputValueClass(Text.class);
		summarizeJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(summarizeJob, new Path(tmp_folder));
		if (!summarizeJob.waitForCompletion(true)) {
			System.exit(1);
		}

		Job sortJob = Job.getInstance(conf, "sort popularity");
		sortJob.setJarByClass(MostPopularAirports.class);
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		// Input
		SequenceFileInputFormat.setInputPaths(sortJob, new Path(tmp_folder));
		// Map & Reduce. Note that reducer is not needed because sorting is done
		// in the mapping phase.
		sortJob.setMapOutputKeyClass(IntWritable.class);
		sortJob.setMapOutputValueClass(Text.class);
		sortJob.setSortComparatorClass(DescendingIntComparator.class);
		sortJob.setNumReduceTasks(1);
		sortJob.setOutputKeyClass(IntWritable.class);
		sortJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(sortJob, new Path(
				"/most_popular_airports"));
		if (!sortJob.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
