package com.capstone.maven.mostontimeairlines;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyValueSwappingMapper extends Mapper<Text, OntimeSummaryWritable, OntimeSummaryWritable, Text>{
	public void map(Text key, OntimeSummaryWritable value, Context context) throws IOException, InterruptedException {
		context.write(value, key);
	}
}
