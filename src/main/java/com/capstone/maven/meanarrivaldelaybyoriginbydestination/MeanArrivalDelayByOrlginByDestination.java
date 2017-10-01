package com.capstone.maven.meanarrivaldelaybyoriginbydestination;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

/**
 * For question 4 in Group 2. For each source-destination pair X-Y, determine
 * the mean arrival delay (in minutes) for a flight from X to Y.
 */
public class MeanArrivalDelayByOrlginByDestination {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job summarizeJob = Job.getInstance(conf, "summarize arrival delay");
		summarizeJob.setJarByClass(MeanArrivalDelayByOrlginByDestination.class);

		// Input
		FileInputFormat.setInputPaths(summarizeJob, new Path(args[0]));
		// Map & Reduce
		summarizeJob.setMapperClass(ArrivalDelaySummaryMapper.class);
		summarizeJob.setMapOutputKeyClass(OriginDestinationGroupKey.class);
		summarizeJob.setMapOutputValueClass(ArrivalDelaySummaryWritable.class);
		summarizeJob.setCombinerClass(ArrivalDelaySummaryReducer.class);
		summarizeJob.setReducerClass(ArrivalDelaySummaryReducer.class);
		// Output
		summarizeJob.setOutputKeyClass(OriginDestinationGroupKey.class);
		summarizeJob.setOutputValueClass(ArrivalDelaySummaryWritable.class);
		FileOutputFormat.setOutputPath(summarizeJob, new Path(
				"/mean_arrival_delay_by_origin_by_destination"));
		if (!summarizeJob.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
