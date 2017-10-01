package com.capstone.maven.meanarrivaldelaybyoriginbydestination;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ArrivalDelaySummaryWritable implements
		WritableComparable<ArrivalDelaySummaryWritable> {
	private DoubleWritable total_delay;
	private IntWritable total_count;

	public ArrivalDelaySummaryWritable() {
		total_delay = new DoubleWritable(0);
		total_count = new IntWritable(0);
	}
	
	public void addOneData(double delay) {
		total_delay.set(total_delay.get() + delay);
		total_count.set(total_count.get() + 1);
	}

	public Double getMeanArrivalDelay() {
		return this.total_delay.get() / this.total_count.get();
	}

	public void combine(ArrivalDelaySummaryWritable o) {
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
		return String.format("%d %.6f", total_count.get(), getMeanArrivalDelay());
	}

	public int compareTo(ArrivalDelaySummaryWritable o) {
		return getMeanArrivalDelay().compareTo(o.getMeanArrivalDelay());
	}
}