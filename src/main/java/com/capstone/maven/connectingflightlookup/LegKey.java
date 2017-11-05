package com.capstone.maven.connectingflightlookup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LegKey implements WritableComparable<LegKey> {
	private Text y;
	private IntWritable daysSinceEpoch;

	private static final DateTimeFormatter fmt = DateTimeFormat
			.forPattern("yyyy-MM-dd");

	public LegKey() {
		this.y = new Text();
		this.daysSinceEpoch = new IntWritable();
	}

	public LegKey(String y, int daysSinceEpoch) {
		this.y = new Text(y);
		this.daysSinceEpoch = new IntWritable(daysSinceEpoch);
	}
	
	public String getY() {
		return this.y.toString();
	}

	public void readFields(DataInput in) throws IOException {
		this.y.readFields(in);
		this.daysSinceEpoch.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.y.write(out);
		this.daysSinceEpoch.write(out);
	}

	@Override
	public String toString() {
		return this.y.toString()
				+ " "
				+ (new DateTime(0).plusDays(this.daysSinceEpoch.get()))
						.toString(fmt);
	}

	public int compareTo(LegKey o) {
		int originCompare = this.y.compareTo(o.y);
		if (originCompare != 0) {
			return originCompare;
		} else {
			return this.daysSinceEpoch.compareTo(o.daysSinceEpoch);
		}
	}

	/*
	 * To make sure same key goes to same reducer.
	 */
	@Override
	public int hashCode() {
		return 31 * this.y.hashCode() + this.daysSinceEpoch.hashCode();
	}
}
