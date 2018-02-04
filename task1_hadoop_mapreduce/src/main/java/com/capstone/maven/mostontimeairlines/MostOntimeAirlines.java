package com.capstone.maven.mostontimeairlines;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * For question 2 in Group 1. List the top 10 airlines by on-time arrival
 * performance.
 */
public class MostOntimeAirlines {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job summarizeJob = Job.getInstance(conf, "summarize ontime rate");
		summarizeJob.setJarByClass(MostOntimeAirlines.class);
		// Input
		FileInputFormat.setInputPaths(summarizeJob, new Path(args[0]));
		// Map & Reduce
		summarizeJob.setMapperClass(OntimeCountingMapper.class);
		summarizeJob.setMapOutputValueClass(OntimeSummaryWritable.class);
		summarizeJob.setCombinerClass(OntimeSummaryReducer.class);
		summarizeJob.setReducerClass(OntimeSummaryReducer.class);
		// Output
		summarizeJob.setOutputKeyClass(Text.class);
		summarizeJob.setOutputValueClass(OntimeSummaryWritable.class);
		summarizeJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(summarizeJob, new Path(
				"/most_ontime_airlines_tmp"));
		if (!summarizeJob.waitForCompletion(true)) {
			System.exit(1);
		}

		Job sortJob = Job.getInstance(conf, "sort ontime rate");
		sortJob.setJarByClass(MostOntimeAirlines.class);
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);
		// Input
		SequenceFileInputFormat.setInputPaths(sortJob, new Path(
				"/most_ontime_airlines_tmp"));
		// Map & Reduce. Note that reducer is not needed because sorting is done
		// in the mapping phase.
		sortJob.setMapperClass(KeyValueSwappingMapper.class);
		sortJob.setMapOutputKeyClass(OntimeSummaryWritable.class);
		sortJob.setMapOutputValueClass(Text.class);
		sortJob.setNumReduceTasks(1);
		sortJob.setOutputKeyClass(OntimeSummaryWritable.class);
		sortJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(sortJob, new Path(
				"/most_ontime_airlines"));
		if (!sortJob.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
