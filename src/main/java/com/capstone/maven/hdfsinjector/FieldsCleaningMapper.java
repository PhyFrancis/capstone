package com.capstone.maven.hdfsinjector;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.capstone.maven.common.AirlineOntimeDataField;

public class FieldsCleaningMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	private static final int[] fields = AirlineOntimeDataField.getAllIndices();
	private static final String delimiter = AirlineOntimeDataField.getFieldDelimiter();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Skip first line of file because that's the header.
		if (key.get() == 0) {
			return;
		}
		
		String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		String output = "";
		for (int field : fields) {
			if (field <= tokens.length) {
				output = output.concat(tokens[field] + delimiter);
			}
		}
		context.write(new Text(output), NullWritable.get());
	}
}