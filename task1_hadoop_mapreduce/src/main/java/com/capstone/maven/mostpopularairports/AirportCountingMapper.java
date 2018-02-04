package com.capstone.maven.mostpopularairports;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.capstone.maven.common.AirlineOntimeDataField;

public class AirportCountingMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	private static final int ORIGIN_FIELD = AirlineOntimeDataField.ORIGIN.getFieldIndexInCleanedFile();
	private static final int DEST_FIELD = AirlineOntimeDataField.DEST.getFieldIndexInCleanedFile();
	
	private final static LongWritable ONE = new LongWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(AirlineOntimeDataField.getFieldDelimiter());
		
		context.write(new Text(tokens[ORIGIN_FIELD]), ONE);
		context.write(new Text(tokens[DEST_FIELD]), ONE);
	}
}