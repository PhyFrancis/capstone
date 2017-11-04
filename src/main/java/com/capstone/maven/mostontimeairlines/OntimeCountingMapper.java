package com.capstone.maven.mostontimeairlines;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.capstone.maven.common.AirlineOntimeDataField;

public class OntimeCountingMapper extends
		Mapper<LongWritable, Text, Text, OntimeSummaryWritable> {
	private static final int UNIQUE_CARRIER = AirlineOntimeDataField.UNIQUE_CARRIER.getFieldIndexInCleanedFile();
	private static final int ARR_DELAY = AirlineOntimeDataField.ARR_DELAY.getFieldIndexInCleanedFile();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(AirlineOntimeDataField.getFieldDelimiter());

		String uniqueCarrier = tokens[UNIQUE_CARRIER];
		double arrDelay  = 0.0;
		if (!tokens[ARR_DELAY].isEmpty()) {
			arrDelay = Double.parseDouble(tokens[ARR_DELAY]);
		}

		context.write(new Text(uniqueCarrier), new OntimeSummaryWritable(arrDelay));
	}
}