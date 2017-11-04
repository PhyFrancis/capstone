package com.capstone.maven.ontimedeparturebyoriginbydestination;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class OntimeSummaryWritable implements
		WritableComparable<OntimeSummaryWritable> {
	private DoubleWritable total_delay;
	private IntWritable total_count;

	public OntimeSummaryWritable() {
		total_delay = new DoubleWritable(0);
		total_count = new IntWritable(0);
	}

	public OntimeSummaryWritable(double delay) {
		total_delay = new DoubleWritable(delay);
		total_count = new IntWritable(1);
	}

	public Double getAverageDelay() {
		return this.total_delay.get() / this.total_count.get();
	}

	public void combine(OntimeSummaryWritable o) {
		total_delay.set(total_delay.get() + o.total_delay.get());
		total_count.set(total_count.get() + o.total_count.get());
	}

	public void readFields(DataInput in) throws IOException {
		total_delay.readFields(in);
		total_count.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		total_delay.write(out);
		total_count.write(out);
	}

	@Override
	public String toString() {
		return String.format("%.6f (%.6f/%d)", this.getAverageDelay(),
				total_delay.get(), total_count.get());
	}

	public int compareTo(OntimeSummaryWritable o) {
		return this.getAverageDelay().compareTo(o.getAverageDelay());
	}
}