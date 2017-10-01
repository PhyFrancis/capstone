package com.capstone.maven.meanarrivaldelaybyoriginbydestination;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OriginDestinationGroupKey implements WritableComparable<OriginDestinationGroupKey> {
	private Text origin;
	private Text destination;
	
	public OriginDestinationGroupKey() {
		origin = new Text();
		destination = new Text();
	}

	public OriginDestinationGroupKey(String origin, String destination) {
		this.origin = new Text(origin);
		this.destination = new Text(destination);
	}

	public void readFields(DataInput in) throws IOException {
		origin.readFields(in);
		destination.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		origin.write(out);
		destination.write(out);
	}

	@Override
	public String toString() {
		return origin.toString() + " " + destination.toString();
	}

	public int compareTo(OriginDestinationGroupKey o) {
		int originCompare = origin.compareTo(o.origin);
		if (originCompare != 0) {
			return originCompare;
		} else {
			return destination.compareTo(o.destination);
		}
	}
	
	/*
	 * To make sure same key goes to same reducer.
	 */
	@Override
	public int hashCode() {
		return 31 * origin.hashCode() + destination.hashCode();
	}
}
