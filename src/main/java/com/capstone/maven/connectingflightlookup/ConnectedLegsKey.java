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

public class ConnectedLegsKey implements WritableComparable<ConnectedLegsKey> {
	private Text x;
	private Text y;
	private Text z;
	private IntWritable daysSinceEpoch;

	private static final DateTimeFormatter fmt = DateTimeFormat
			.forPattern("yyyy-MM-dd");

	public ConnectedLegsKey() {
		this.x = new Text();
		this.y = new Text();
		this.z = new Text();
		this.daysSinceEpoch = new IntWritable();
	}

	public ConnectedLegsKey(String x, String y, String z, int daysSinceEpoch) {
		this.x = new Text(x);
		this.y = new Text(y);
		this.z = new Text(z);
		this.daysSinceEpoch = new IntWritable(daysSinceEpoch);
	}

	public void readFields(DataInput in) throws IOException {
		this.x.readFields(in);
		this.y.readFields(in);
		this.z.readFields(in);
		this.daysSinceEpoch.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.x.write(out);
		this.y.write(out);
		this.z.write(out);
		this.daysSinceEpoch.write(out);
	}

	@Override
	public String toString() {
		final String date = (new DateTime(0)
				.plusDays(this.daysSinceEpoch.get())).toString(fmt);
		return String.format("%s-%s-%s %s", x.toString(), y.toString(),
				z.toString(), date);
	}

	public int compareTo(ConnectedLegsKey o) {
		if (x.compareTo(o.x) != 0) {
			return x.compareTo(o.x);
		} else if (y.compareTo(o.y) != 0) {
			return y.compareTo(o.y);
		} else if (z.compareTo(o.z) != 0) {
			return z.compareTo(o.z);
		} else {
			return daysSinceEpoch.compareTo(o.daysSinceEpoch);
		}
	}

	/*
	 * To make sure same key goes to same reducer.
	 */
	@Override
	public int hashCode() {
		return 31 * (this.x.hashCode() + this.y.hashCode() + this.z.hashCode())
				+ this.daysSinceEpoch.hashCode();
	}
}
