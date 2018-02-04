package com.capstone.maven.common;

public enum AirlineOntimeDataField {
	YEAR(0),
	MONTH(2),
	DAY_OF_MONTH(3),
	DAY_OF_WEEK(4),
	FLIGHT_DATE(5),
	UNIQUE_CARRIER(6),
	AIRLINE_ID(7),
	CARRIER(8),
	FLIGHT_NUM(10),
	ORIGIN(11),
	DEST(17),
	CRS_DEP_TIME(23),
	DEP_DELAY(25),
	CRS_ARR_TIME(34),
	ARR_DELAY(36),
	CANCELLED(41),
	DIVERTED(43),
	FLIGHTS(47);
	
	public final int id;
	
	AirlineOntimeDataField(int id) {
		this.id = id;
	}
	
	public int getFieldIndexInRawFile() {
		return this.id;
	}
	
	public int getFieldIndexInCleanedFile() {
		return this.ordinal();
	}
	
	public static int[] getAllIndices() {
		return new int[]{0, 2, 3, 4, 5, 6, 7, 8, 10, 11, 17, 23, 25, 34, 36, 41, 43, 47};
	}
	
	public static String getFieldDelimiter() {
		return ":::";
	}
}
