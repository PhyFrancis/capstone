package com.capstone.maven.meanarrivaldelaybyoriginbydestination;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.capstone.maven.common.AirlineOntimeDataField;

public class ArrivalDelaySummaryMapper
		extends
		Mapper<LongWritable, Text, OriginDestinationGroupKey, ArrivalDelaySummaryWritable> {
	private static final int DESTINATION = AirlineOntimeDataField.DEST
			.getFieldIndexInCleanedFile();
	private static final int ORIGIN = AirlineOntimeDataField.ORIGIN
			.getFieldIndexInCleanedFile();
	private static final int ARR_DELAY = AirlineOntimeDataField.ARR_DELAY
			.getFieldIndexInCleanedFile();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(
				AirlineOntimeDataField.getFieldDelimiter());

		Double arrDelay = 0.0;
		if (!tokens[ARR_DELAY].isEmpty()) {
			arrDelay = Double.valueOf(tokens[ARR_DELAY]);
		}

		ArrivalDelaySummaryWritable ontimeSummary = new ArrivalDelaySummaryWritable();
		if (arrDelay <= 0) {
			ontimeSummary.addOneData(0.0);
		} else {
			ontimeSummary.addOneData(arrDelay);
		}

		context.write(new OriginDestinationGroupKey(tokens[ORIGIN],
				tokens[DESTINATION]), ontimeSummary);
	}
}