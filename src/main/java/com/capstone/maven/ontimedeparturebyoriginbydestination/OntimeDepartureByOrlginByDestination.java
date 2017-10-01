package com.capstone.maven.ontimedeparturebyoriginbydestination;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

/**
 * For question 2 in Group 2. For each airport, list the top 10 destination
 * airports by on-time departure performance.
 */
public class OntimeDepartureByOrlginByDestination {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job summarizeJob = Job.getInstance(conf, "summarize ontime rate");
		summarizeJob.setJarByClass(OntimeDepartureByOrlginByDestination.class);

		// Input
		FileInputFormat.setInputPaths(summarizeJob, new Path(args[0]));
		// Map & Reduce
		summarizeJob.setMapperClass(OntimeCountingMapper.class);
		summarizeJob.setMapOutputKeyClass(OriginDestinationGroupKey.class);
		summarizeJob.setMapOutputValueClass(OntimeSummaryWritable.class);
		summarizeJob.setCombinerClass(OntimeSummaryReducer.class);
		summarizeJob.setReducerClass(OntimeSummaryReducer.class);
		// Output
		summarizeJob.setOutputKeyClass(OriginDestinationGroupKey.class);
		summarizeJob.setOutputValueClass(OntimeSummaryWritable.class);
		FileOutputFormat.setOutputPath(summarizeJob, new Path(
				"/departure_ontime_by_origin_by_destination"));
		if (!summarizeJob.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
