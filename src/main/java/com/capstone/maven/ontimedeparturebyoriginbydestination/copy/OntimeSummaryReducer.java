package com.capstone.maven.ontimedeparturebyoriginbydestination.copy;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class OntimeSummaryReducer extends
		Reducer<OriginDestinationGroupKey, OntimeSummaryWritable, OriginDestinationGroupKey, OntimeSummaryWritable> {
	@Override
	public void reduce(OriginDestinationGroupKey key, Iterable<OntimeSummaryWritable> values,
			Context context) throws IOException, InterruptedException {
		OntimeSummaryWritable summary = new OntimeSummaryWritable();
		for (OntimeSummaryWritable val : values) {
			summary.combine(val);
		}
		context.write(key, summary);
	}
}
