package com.capstone.maven.mostontimeairlines;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OntimeSummaryReducer extends
		Reducer<Text, BooleanWritable, Text, OntimeSummaryWritable> {
	@Override
	public void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
		OntimeSummaryWritable summary = new OntimeSummaryWritable();
		for (BooleanWritable val : values) {
			if (val.get()) {
				summary.incrementOntime();
			} else {
				summary.incrementNotOntime();
			}
		}
		context.write(key, summary);
	}
}
