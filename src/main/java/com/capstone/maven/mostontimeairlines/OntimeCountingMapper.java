package com.capstone.maven.mostontimeairlines;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.capstone.maven.common.AirlineOntimeDataField;

public class OntimeCountingMapper extends
		Mapper<LongWritable, Text, Text, OntimeSummaryWritable> {
	private static final int UNIQUE_CARRIER = AirlineOntimeDataField.UNIQUE_CARRIER
			.getFieldIndexInCleanedFile();
	private static final int ARR_DELAY = AirlineOntimeDataField.ARR_DELAY
			.getFieldIndexInCleanedFile();
	private static final int CANCELLED = AirlineOntimeDataField.CANCELLED
			.getFieldIndexInCleanedFile();
	private static final int DIVERTED = AirlineOntimeDataField.DIVERTED
			.getFieldIndexInCleanedFile();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(
				AirlineOntimeDataField.getFieldDelimiter());

		if (Double.parseDouble(tokens[CANCELLED]) > 0
				|| Double.parseDouble(tokens[DIVERTED]) > 0
				|| tokens[ARR_DELAY].isEmpty()) {
			return;
		}

		context.write(
				new Text(tokens[UNIQUE_CARRIER]),
				new OntimeSummaryWritable(Double.parseDouble(tokens[ARR_DELAY])));
	}
}