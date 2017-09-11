package com.capstone.maven.mostontimeairlines;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class OntimeSummaryWritable implements Writable {
	private IntWritable ontime_count;
	private IntWritable total_count;

	public OntimeSummaryWritable() {
		ontime_count.set(0);
		total_count.set(0);
	}

	public void incrementOntime() {
		ontime_count.set(ontime_count.get() + 1);
		total_count.set(total_count.get() + 1);
	}

	public void incrementNotOntime() {
		total_count.set(total_count.get() + 1);
	}

	public Double getOntimeRate() {
		return (double) this.ontime_count.get() / this.total_count.get();
	}

	public void readFields(DataInput in) throws IOException {
		ontime_count.readFields(in);
		total_count.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		ontime_count.write(out);
		total_count.write(out);
	}
}
