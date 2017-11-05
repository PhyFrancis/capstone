package com.capstone.maven.connectingflightlookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.capstone.maven.common.AirlineOntimeDataField;

public class ConnectingLegsReducer extends
		Reducer<LegKey, Text, Text, NullWritable> {
	private static final int CARRIER = AirlineOntimeDataField.CARRIER
			.getFieldIndexInCleanedFile();
	private static final int FLIGHT_NUM = AirlineOntimeDataField.FLIGHT_NUM
			.getFieldIndexInCleanedFile();
	private static final int ORIGIN = AirlineOntimeDataField.ORIGIN
			.getFieldIndexInCleanedFile();
	private static final int DEST = AirlineOntimeDataField.DEST
			.getFieldIndexInCleanedFile();
	private static final int FLIGHT_DATE = AirlineOntimeDataField.FLIGHT_DATE
			.getFieldIndexInCleanedFile();
	private static final int ARR_DELAY = AirlineOntimeDataField.ARR_DELAY
			.getFieldIndexInCleanedFile();

	private static final DateTimeFormatter fmt = DateTimeFormat
			.forPattern("yyyy-MM-dd");

	public class FlightInfo {
		final private String uniqueFlightNum;
		final private String origin;
		final private String dest;
		final private double arrDelay;
		final private DateTime date;

		public FlightInfo(Text text) {
			String[] tokens = text.toString().split(AirlineOntimeDataField.getFieldDelimiter());
			this.uniqueFlightNum = (tokens[CARRIER] + tokens[FLIGHT_NUM]).replace("\"", "");
			this.origin = tokens[ORIGIN];
			this.dest = tokens[DEST];
			this.arrDelay = Double.parseDouble(tokens[ARR_DELAY]);
			this.date = fmt.parseDateTime(tokens[FLIGHT_DATE]);
		}
	}

	@Override
	public void reduce(LegKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<FlightInfo> xyLegs = new ArrayList<FlightInfo>();
		List<FlightInfo> yzLegs = new ArrayList<FlightInfo>();
		for (Text val : values) {
			FlightInfo info = new FlightInfo(val);
			if (info.origin.equals(key.getY())) {
				yzLegs.add(info);
			} else if (info.dest.equals(key.getY())) {
				xyLegs.add(info);
			}
		}
		if (xyLegs.isEmpty() || yzLegs.isEmpty()) {
			return;
		}
		for (FlightInfo xyLeg : xyLegs) {
			for (FlightInfo yzLeg : yzLegs) {
				context.write(
						new Text(String.format(
								"%s %s %s %s %s %s %s %.3f",
								xyLeg.origin, xyLeg.dest, yzLeg.dest,
								xyLeg.date, xyLeg.uniqueFlightNum, yzLeg.date,
								yzLeg.uniqueFlightNum, xyLeg.arrDelay
										+ yzLeg.arrDelay)), NullWritable.get());
			}
		}
	}
}
