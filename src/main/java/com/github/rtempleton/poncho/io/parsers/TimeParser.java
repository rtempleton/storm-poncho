package com.github.rtempleton.poncho.io.parsers;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimeParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private final static Logger logger = Logger.getLogger(TimeParser.class);
	private final static String DEFAULT_FORMAT = "HH:mm:ss";
	private final DateTimeFormatter dtf;
	private final String name;
	private Object nullValue;
	
	public TimeParser(String name, String simpleDateFormat, String nullVal) {
		String format = (simpleDateFormat==null || simpleDateFormat.isEmpty()) ? DEFAULT_FORMAT : simpleDateFormat;
		dtf = DateTimeFormat.forPattern(format).withChronology(ISOChronology.getInstanceUTC());
		this.name=name;
		if(nullVal == null ||nullVal.isEmpty())
			nullValue = null;
		else{
			try {
				DateTime dt = dtf.parseDateTime(nullVal.trim());
				nullValue = new LocalTime(dt.getMillisOfDay());
			} catch (Exception e) {
				nullValue = null;
			}
		}
	}

	public Object parse(String token) {
		try{
			DateTime dt = dtf.parseDateTime(token.trim());
			return new LocalTime(dt.getMillisOfDay());
		}catch(Exception e){
			logger.warn(e.getMessage());
			logger.warn(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
	}

}
