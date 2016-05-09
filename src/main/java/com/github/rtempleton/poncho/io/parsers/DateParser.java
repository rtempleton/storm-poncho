package com.github.rtempleton.poncho.io.parsers;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private final static Logger logger = Logger.getLogger(DateParser.class);
	private final static String DEFAULT_FORMAT = "yyyy-MM-dd";
	private final DateTimeFormatter dtf;
	private final String name;
	
	public DateParser(String name, String simpleDateFormat){
		String format = (simpleDateFormat==null || simpleDateFormat.isEmpty()) ? DEFAULT_FORMAT : simpleDateFormat;
		dtf = DateTimeFormat.forPattern(format).withOffsetParsed().withPivotYear(2000).withChronology(ISOChronology.getInstance());
		this.name=name;
	}

	public Object parse(String token) {
		try {
			DateTime dt = dtf.parseDateTime(token.trim());
			return new LocalDate(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
		} catch (Exception e) {
			logger.warn(e.getMessage());
			logger.warn(String.format("Error parsing token %s at field %s. Pushing null instead.", token, name));
			return null;
		}
	}

}
