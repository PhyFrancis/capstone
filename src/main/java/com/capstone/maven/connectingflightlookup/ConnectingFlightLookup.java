package com.capstone.maven.connectingflightlookup;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * For question 4 in Group 2. For each source-destination pair X-Y, determine
 * the mean arrival delay (in minutes) for a flight from X to Y.
 */
public class ConnectingFlightLookup {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job lookupJob = Job.getInstance(conf, "lookup connecting flight");
		lookupJob.setJarByClass(ConnectingFlightLookup.class);

		// Input
		FileInputFormat.setInputPaths(lookupJob, new Path(args[0]));
		// Map & Reduce
		lookupJob.setMapperClass(LegGeneratorMapper.class);
		lookupJob.setMapOutputKeyClass(LegKey.class);
		lookupJob.setMapOutputValueClass(Text.class);
		lookupJob.setReducerClass(ConnectingLegsReducer.class);

		// Output
		lookupJob.setOutputKeyClass(Text.class);
		lookupJob.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(lookupJob, new Path(
				"/connecting_flight_legs"));
		if (!lookupJob.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
