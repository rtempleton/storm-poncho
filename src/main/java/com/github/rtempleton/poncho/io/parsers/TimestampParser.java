package com.github.rtempleton.poncho.io.parsers;

import org.apache.log4j.Logger;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimestampParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private final static Logger logger = Logger.getLogger(TimestampParser.class);
	private final static String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final DateTimeFormatter dtf;
	private final String name;
	private Object nullValue;
	
	public TimestampParser(String name, String simpleDateFormat, String nullVal) {
		String format = (simpleDateFormat==null || simpleDateFormat.isEmpty()) ? DEFAULT_FORMAT : simpleDateFormat;
		dtf = DateTimeFormat.forPattern(format).withOffsetParsed().withPivotYear(2000).withChronology(ISOChronology.getInstance());
		this.name=name;
		try {
			nullValue = (nullVal == null || nullVal.isEmpty()) ? null : dtf.parseDateTime(nullVal.trim());
		} catch (Exception e) {
			nullValue = null;
		}
		
	}

	public Object parse(String token) {
		if(token.length()>0){
			try{
				return dtf.parseDateTime(token.trim());
			}catch(Exception e){
				logger.warn(e.getMessage());
				logger.warn(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
				return nullValue;
			}
		}
		return null;
	}

}
