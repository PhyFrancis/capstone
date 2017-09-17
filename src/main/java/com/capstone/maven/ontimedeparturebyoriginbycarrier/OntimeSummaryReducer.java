package com.capstone.maven.ontimedeparturebyoriginbycarrier;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OntimeSummaryReducer extends
		Reducer<Text, OntimeSummaryWritable, Text, OntimeSummaryWritable> {
	@Override
	public void reduce(Text key, Iterable<OntimeSummaryWritable> values, Context context) throws IOException, InterruptedException {
		OntimeSummaryWritable summary = new OntimeSummaryWritable();
		for (OntimeSummaryWritable val : values) {
			summary.combine(val);
		}
		context.write(key, summary);
	}
}
