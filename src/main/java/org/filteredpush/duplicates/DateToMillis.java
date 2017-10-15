// Copyright 2013 Harvard University

package org.filteredpush.duplicates;
//import java.util.Date;
//import java.text.DateFormat;
import java.text.ParseException;
//import java.text.SimpleDateFormat;

/** @author Robert A. Morris
 * 
 * Provision of DateTime as a real number as time from an agreed starting point (the EPOCH)
 */

/*
 * @TODO support a normalized form that takes normalization factor and maybe EPOCH as argument. 
 * Probably this means refactor away from all the static representation and replace with some Object
 * with final values for the normalization and the EPOCH.
 */
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateToMillis {
	static final long CALENDAR_PERIOD = 400; 
	static final long EPOCH = 1970;
	static final String EPOCH_STR = "1970-01-01T00:00:00Z";
	
	
	static final long FIRSTYEAR = EPOCH-CALENDAR_PERIOD ; //1970 - 400 = 1570
	static final long LASTYEAR = EPOCH+CALENDAR_PERIOD ; //1970 + 400 = 2370				s
	static final long maxYears = LASTYEAR; //years
	static final long minPerYear = 525949; //umm not leap years
	static final long millisPerYear = minPerYear*60*1000; //umm not leap years
	static double maxMins = maxYears*minPerYear;
	static final String defaultPattern = "yyyy-MM-ddTHH:mmZ";
	static final String MAX_DATE = "2370-01-01T00:00:00Z"; //should be at 1.0 on time axis when normalized
	static final String MIN_DATE = "1570-01-01T00:00:00Z"; //should be at -1.0 on the time axis when normalized 
	static final String EPOCH_DATE = "1970-01-01T00:00:00Z";
	static final long maxMillis =  millisFromEpoch(MAX_DATE); //normalization factor in milliseconds
	static final double millisNorm = 1.0/(double)maxMillis;
		
	public static long millisFromEpoch(String source) {
		DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();
		DateTime dt = fmt.parseDateTime(source);
		return millisFromEpoch(dt);
	}
public static long millisFromEpoch(DateTime source) {
		return source.toInstant().getMillis();
	}
	
	public static double normalizedMillisFromEpoch(String source) {
		return ((double)millisFromEpoch(source)/(double)maxMillis);
	}
	public static double normalizedMillisFromEpoch(DateTime source) {
		return ((double)source.toInstant().getMillis())/(double)maxMillis;
	}	
}
