package com.capstone.maven.ontimedeparturebyoriginbycarrier;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

/**
 * For question 1 in Group 2. For each airport, list the top 10 airlines by
 * on-time departure performance.
 */
public class OntimeDepartureByOriginByCarrier {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job summarizeJob = Job.getInstance(conf, "summarize ontime rate");
		summarizeJob.setJarByClass(OntimeDepartureByOriginByCarrier.class);

		// Input
		FileInputFormat.setInputPaths(summarizeJob, new Path(args[0]));
		// Map & Reduce
		summarizeJob.setMapperClass(OntimeCountingMapper.class);
		summarizeJob.setMapOutputKeyClass(OriginCarrierGroupKey.class);
		summarizeJob.setMapOutputValueClass(OntimeSummaryWritable.class);
		summarizeJob.setCombinerClass(OntimeSummaryReducer.class);
		summarizeJob.setReducerClass(OntimeSummaryReducer.class);
		// Output
		summarizeJob.setOutputKeyClass(OriginCarrierGroupKey.class);
		summarizeJob.setOutputValueClass(OntimeSummaryWritable.class);
		FileOutputFormat.setOutputPath(summarizeJob, new Path(
				"/departure_ontime_by_origin_by_carrier"));
		if (!summarizeJob.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
