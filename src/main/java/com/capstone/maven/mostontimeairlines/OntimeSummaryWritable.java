package com.capstone.maven.mostontimeairlines;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class OntimeSummaryWritable implements
		WritableComparable<OntimeSummaryWritable> {
	private IntWritable count;
	private DoubleWritable totalDelay;

	private Double getAverageDelay() {
		return this.totalDelay.get() / this.count.get();
	}

	public OntimeSummaryWritable() {
		count = new IntWritable(0);
		totalDelay = new DoubleWritable(0.0);
	}
	
	public OntimeSummaryWritable(double delay) {
		count = new IntWritable(1);
		totalDelay = new DoubleWritable(delay);
	}

	public void readFields(DataInput in) throws IOException {
		count.readFields(in);
		totalDelay.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		count.write(out);
		totalDelay.write(out);
	}
	
	public void combine(OntimeSummaryWritable o) {
		this.count.set(this.count.get() + o.count.get());
		this.totalDelay.set(this.totalDelay.get() + o.totalDelay.get());
	}
	
	@Override
	public String toString() {
		return String.format("Average Delay: %.6f / %d = %.6f", totalDelay.get(),
				count.get(), getAverageDelay());
	}

	public int compareTo(OntimeSummaryWritable o) {
		return getAverageDelay().compareTo(o.getAverageDelay());
	}
}
