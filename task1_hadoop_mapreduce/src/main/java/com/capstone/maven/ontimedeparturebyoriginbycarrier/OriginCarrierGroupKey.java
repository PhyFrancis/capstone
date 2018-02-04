package com.capstone.maven.ontimedeparturebyoriginbycarrier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OriginCarrierGroupKey implements WritableComparable<OriginCarrierGroupKey> {
	private Text origin;
	private Text carrier;
	
	public OriginCarrierGroupKey() {
		origin = new Text();
		carrier = new Text();
	}

	public OriginCarrierGroupKey(String origin, String carrier) {
		this.origin = new Text(origin);
		this.carrier = new Text(carrier);
	}

	public void readFields(DataInput in) throws IOException {
		origin.readFields(in);
		carrier.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		origin.write(out);
		carrier.write(out);
	}

	@Override
	public String toString() {
		return origin.toString() + " " + carrier.toString();
	}

	public int compareTo(OriginCarrierGroupKey o) {
		int originCompare = origin.compareTo(o.origin);
		if (originCompare != 0) {
			return originCompare;
		} else {
			return carrier.compareTo(o.carrier);
		}
	}
	
	/*
	 * To make sure same key goes to same reducer.
	 */
	@Override
	public int hashCode() {
		return 31 * origin.hashCode() + carrier.hashCode();
	}
}
