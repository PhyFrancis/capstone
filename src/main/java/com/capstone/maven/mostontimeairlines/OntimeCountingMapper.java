package com.capstone.maven.mostontimeairlines;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.capstone.maven.common.AirlineOntimeDataField;

public class OntimeCountingMapper extends
		Mapper<LongWritable, Text, Text, BooleanWritable> {
	private static final int UNIQUE_CARRIER = AirlineOntimeDataField.UNIQUE_CARRIER.getFieldIndexInCleanedFile();
	private static final int ARR_DELAY = AirlineOntimeDataField.ARR_DELAY.getFieldIndexInCleanedFile();
	
	private final static BooleanWritable IS_ONTIME = new BooleanWritable(true);
	private final static BooleanWritable NOT_ONTIME = new BooleanWritable(false);
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(AirlineOntimeDataField.getFieldDelimiter());

		String uniqueCarrier = tokens[UNIQUE_CARRIER];
		Double arrDelay  = 0.0;
		if (!tokens[ARR_DELAY].isEmpty()) {
			arrDelay = Double.valueOf(tokens[ARR_DELAY]);
		}

		if (arrDelay <= 0) {
			context.write(new Text(uniqueCarrier), IS_ONTIME);
		} else {
			context.write(new Text(uniqueCarrier), NOT_ONTIME);
		}
	}
}