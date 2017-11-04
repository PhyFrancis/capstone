package com.capstone.maven.mostontimeairlines;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OntimeSummaryReducer extends
		Reducer<Text, OntimeSummaryWritable, Text, OntimeSummaryWritable> {
	@Override
	public void reduce(Text key, Iterable<OntimeSummaryWritable> values, Context context) throws IOException, InterruptedException {
		OntimeSummaryWritable summary = new OntimeSummaryWritable();
		for (OntimeSummaryWritable value : values) {
			summary.combine(value);
		}
		context.write(key, summary);
	}
}
