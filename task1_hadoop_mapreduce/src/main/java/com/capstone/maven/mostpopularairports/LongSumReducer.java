package com.capstone.maven.mostpopularairports;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LongSumReducer extends
		Reducer<Text, LongWritable, LongWritable, Text> {
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		context.write(new LongWritable(sum), key);
	}
}
