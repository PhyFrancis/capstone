package com.capstone.maven.mostpopularairports;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * For question 1 in Group 1.
 */
public class MostPopularAirports {
	public static void main(String[] args) throws Exception {
		Configuration jobConf = new Configuration();
		Job summarizeJob = Job.getInstance(jobConf, "most popular airports");
		summarizeJob.setJarByClass(MostPopularAirports.class);
		final String tmp_folder = "/most_popular_airports_tmp";

		// Input
		FileInputFormat.setInputPaths(summarizeJob, new Path(args[0]));
		// Map & Reduce
		summarizeJob.setMapperClass(AirportCountingMapper.class);
		summarizeJob.setMapOutputKeyClass(Text.class);
		summarizeJob.setMapOutputValueClass(LongWritable.class);
		summarizeJob.setReducerClass(LongSumReducer.class);
		// Output
		summarizeJob.setOutputKeyClass(LongWritable.class);
		summarizeJob.setOutputValueClass(Text.class);
		summarizeJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(summarizeJob, new Path(tmp_folder));
		if (!summarizeJob.waitForCompletion(true)) {
			System.exit(1);
		}

		Job sortJob = Job.getInstance(jobConf, "sort popularity");
		sortJob.setJarByClass(MostPopularAirports.class);
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		// Input
		SequenceFileInputFormat.setInputPaths(sortJob, new Path(tmp_folder));
		// Map & Reduce. Note that reducer is not needed because sorting is done
		// in the mapping phase.
		sortJob.setNumReduceTasks(1);
		sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		sortJob.setOutputKeyClass(LongWritable.class);
		sortJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(sortJob, new Path(
				"/most_popular_airports"));
		if (!sortJob.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
