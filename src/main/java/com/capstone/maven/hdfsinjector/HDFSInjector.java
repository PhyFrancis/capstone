package com.capstone.maven.hdfsinjector;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Read in unzipped raw csv file, remove those columns that are useless, and
 * store the cleaned data in hdfs
 */
public class HDFSInjector {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HDFS injector");
		job.setJarByClass(HDFSInjector.class);

		// Input
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// Map & Reduce
		job.setMapperClass(FieldsCleaningMapper.class);
		job.setCombinerClass(WritingToHdfsReducer.class);
		job.setReducerClass(WritingToHdfsReducer.class);

		// Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
