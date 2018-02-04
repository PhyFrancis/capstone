package com.capstone.maven.meanarrivaldelaybyoriginbydestination;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class ArrivalDelaySummaryReducer extends
		Reducer<OriginDestinationGroupKey, ArrivalDelaySummaryWritable, OriginDestinationGroupKey, ArrivalDelaySummaryWritable> {
	@Override
	public void reduce(OriginDestinationGroupKey key, Iterable<ArrivalDelaySummaryWritable> values,
			Context context) throws IOException, InterruptedException {
		ArrivalDelaySummaryWritable summary = new ArrivalDelaySummaryWritable();
		for (ArrivalDelaySummaryWritable val : values) {
			summary.combine(val);
		}
		context.write(key, summary);
	}
}
