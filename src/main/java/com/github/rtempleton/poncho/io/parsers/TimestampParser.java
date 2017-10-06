package com.github.rtempleton.poncho.io.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimestampParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(TimestampParser.class);
	private final static String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private final DateTimeFormatter dtf;
	private final String name;
	private Object nullValue;
	
	public TimestampParser(String name, String simpleDateFormat, Object nullVal) {
		this.name=name;
		String format = (simpleDateFormat==null || simpleDateFormat.isEmpty()) ? DEFAULT_FORMAT : simpleDateFormat;
		dtf = DateTimeFormat.forPattern(format).withOffsetParsed().withPivotYear(2000).withChronology(ISOChronology.getInstance());
		
		if(nullVal == null)
			nullValue = null;
		else if (!(nullVal instanceof java.sql.Timestamp))
			nullValue = parse(nullVal);
		else
			nullValue = nullVal;
		
	}

	public Object parse(Object token) {
		
		if(token==null)
			return nullValue;
		
		try{
			if (!(token instanceof java.sql.Timestamp)) {
				String t=token.toString();
				if(t.trim().length()==0)
					return nullValue;
				DateTime dt = dtf.parseDateTime(t.trim());
				return new Timestamp(dt.getMillis());
			}else {
				return token;
			}
		}catch(Exception e){
			logger.debug(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
		

	}

}
