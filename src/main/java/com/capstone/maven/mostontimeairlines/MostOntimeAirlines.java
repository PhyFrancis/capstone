package com.capstone.maven.mostontimeairlines;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

/**
 * For question 2 in Group 1. List the top 10 airlines by on-time arrival performance.
 */
public class MostOntimeAirlines {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("topNCount", "10"); // Only get top 10 ontime arrival airlines. 
		Job job = Job.getInstance(conf, "most popular airports");
		job.setJarByClass(MostOntimeAirlines.class);

		// Input
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// Map & Reduce
		job.setMapperClass(OntimeCountingMapper.class);
		job.setMapOutputValueClass(BooleanWritable.class);
		job.setReducerClass(OntimeSummaryReducer.class);

		// Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OntimeSummaryWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
