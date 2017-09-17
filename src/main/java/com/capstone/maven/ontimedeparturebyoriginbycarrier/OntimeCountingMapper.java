package com.capstone.maven.ontimedeparturebyoriginbycarrier;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.capstone.maven.common.AirlineOntimeDataField;

public class OntimeCountingMapper extends
		Mapper<LongWritable, Text, OriginCarrierGroupKey, OntimeSummaryWritable> {
	private static final int UNIQUE_CARRIER = AirlineOntimeDataField.UNIQUE_CARRIER.getFieldIndexInCleanedFile();
	private static final int ORIGIN = AirlineOntimeDataField.ORIGIN.getFieldIndexInCleanedFile();
	private static final int DEP_DELAY = AirlineOntimeDataField.DEP_DELAY.getFieldIndexInCleanedFile();
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(AirlineOntimeDataField.getFieldDelimiter());

		Double depDelay  = 0.0;
		if (!tokens[DEP_DELAY].isEmpty()) {
			depDelay = Double.valueOf(tokens[DEP_DELAY]);
		}

		OntimeSummaryWritable ontimeSummary = new OntimeSummaryWritable();
		if (depDelay <= 0) {
			ontimeSummary.incrementOntime();
		} else {
			ontimeSummary.incrementNotOntime();
		}

		context.write(new OriginCarrierGroupKey(tokens[ORIGIN], tokens[UNIQUE_CARRIER]), ontimeSummary);
	}
}