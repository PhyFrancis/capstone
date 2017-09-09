package com.capstone.maven.mostontimeairlines.copy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class OntimeSummaryWritable implements Writable {
	private int ontime_count;
	private int total_count;
	
	public OntimeSummaryWritable() {
		this.ontime_count = 0;
		this.total_count = 0;
	}
	
	public void incrementOntime() {
		this.ontime_count += 1;
		this.total_count += 1;
	}
	
	public void incrementNotOntime() {
		this.total_count += 1;
	}
	
	public Double getOntimeRate() {
		return (double)this.ontime_count / this.total_count;
	}

	public void readFields(DataInput dataInput) throws IOException {
		throw new IOException("Should never read an ontime summary.");
	}

	public void write(DataOutput dataOutput) throws IOException {
		Text output = 
				new Text(
						String.format(
								"ontime rate: %d / %d = %f", 
								this.ontime_count, 
								this.total_count, 
								(float)this.ontime_count / this.total_count));
		output.write(dataOutput);
	}
}
