package com.capstone.maven.ontimedeparturebyoriginbydestination.copy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class OntimeSummaryWritable implements
		WritableComparable<OntimeSummaryWritable> {
	private IntWritable ontime_count;
	private IntWritable total_count;

	public OntimeSummaryWritable() {
		ontime_count = new IntWritable(0);
		total_count = new IntWritable(0);
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

	public void combine(OntimeSummaryWritable o) {
		ontime_count.set(ontime_count.get() + o.ontime_count.get());
		total_count.set(total_count.get() + o.total_count.get());
	}

	public void readFields(DataInput in) throws IOException {
		ontime_count.readFields(in);
		total_count.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		ontime_count.write(out);
		total_count.write(out);
	}

	@Override
	public String toString() {
		return String.format("%d %d %.6f", ontime_count.get(),
				total_count.get(),
				(double) ontime_count.get() / total_count.get());
	}

	public int compareTo(OntimeSummaryWritable o) {
		Double ontime_rate = (double) ontime_count.get() / total_count.get();
		Double ontime_rate_that = (double) o.ontime_count.get()
				/ o.total_count.get();
		return ontime_rate_that.compareTo(ontime_rate);
	}
}