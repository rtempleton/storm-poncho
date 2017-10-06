package com.github.rtempleton.poncho.io.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;

import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimeParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(TimeParser.class);
	private final static String DEFAULT_FORMAT = "HH:mm:ss";
	private final DateTimeFormatter dtf;
	private final String name;
	private Object nullValue;
	
	public TimeParser(String name, String simpleDateFormat, Object nullVal) {
		this.name=name;
		String format = (simpleDateFormat==null || simpleDateFormat.isEmpty()) ? DEFAULT_FORMAT : simpleDateFormat;
		dtf = DateTimeFormat.forPattern(format).withChronology(ISOChronology.getInstanceUTC());
		
		if(nullVal == null)
			nullValue = null;
		else if (!(nullVal instanceof java.sql.Time))
			nullValue = parse(nullVal);
		else
			nullValue = nullVal;
	}

	public Object parse(Object token) {
		
		if(token==null)
			return nullValue;
		
		try{
			if (!(token instanceof java.sql.Time)) {
				String t = token.toString();
				if(t.trim().length()==0)
					return nullValue;
				DateTime dt = dtf.parseDateTime(t.trim());
				return new Time(dt.getMillis());
//				return new LocalTime(dt.getMillisOfDay());
			}else {
				return token;
			}
			
		}catch(Exception e){
			logger.debug(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
	}

}
