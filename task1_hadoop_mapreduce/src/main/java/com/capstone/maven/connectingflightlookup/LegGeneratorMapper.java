package com.capstone.maven.connectingflightlookup;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.capstone.maven.common.AirlineOntimeDataField;

public class LegGeneratorMapper extends
		Mapper<LongWritable, Text, LegKey, Text> {
	private static final int FLIGHT_DATE = AirlineOntimeDataField.FLIGHT_DATE
			.getFieldIndexInCleanedFile();
	private static final int DEST = AirlineOntimeDataField.DEST
			.getFieldIndexInCleanedFile();
	private static final int ORIGIN = AirlineOntimeDataField.ORIGIN
			.getFieldIndexInCleanedFile();
	private static final int CRS_DEP_TIME = AirlineOntimeDataField.CRS_DEP_TIME
			.getFieldIndexInCleanedFile();
	private static final int CANCELLED = AirlineOntimeDataField.CANCELLED
			.getFieldIndexInCleanedFile();
	private static final int DIVERTED = AirlineOntimeDataField.DIVERTED
			.getFieldIndexInCleanedFile();

	private static final DateTimeFormatter fmt = DateTimeFormat
			.forPattern("yyyy-MM-dd");
	private static final DateTime EPOCH = new DateTime(0);
	private static final DateTime YZ_LOWER = new DateTime(2008, 1, 2, 0, 0);
	private static final DateTime XY_UPPER = new DateTime(2008, 12, 30, 0, 0);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split(
				AirlineOntimeDataField.getFieldDelimiter());

		DateTime t = fmt.parseDateTime(tokens[FLIGHT_DATE]);
		if (t.getYear() != 2008 || Double.parseDouble(tokens[CANCELLED]) > 0
				|| Double.parseDouble(tokens[DIVERTED]) > 0) {
			return;
		}

		int daysSinceEpoch = Days.daysBetween(EPOCH, t).getDays();
		int crsDepTime = Integer.parseInt(tokens[CRS_DEP_TIME]
				.replace("\"", ""));

		// Generate X-Y leg iff departing before noon
		if (crsDepTime < 1200 && t.isBefore(XY_UPPER)) {
			context.write(new LegKey(tokens[DEST], daysSinceEpoch + 1), value);
		}

		// Generate Y-Z leg iff departing after noon
		if (crsDepTime > 1200 && t.isAfter(YZ_LOWER)) {
			context.write(new LegKey(tokens[ORIGIN], daysSinceEpoch - 1), value);
		}
	}
}