package com.github.rtempleton.poncho.io.parsers;

import java.sql.Time;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

public class TimeParser implements TokenParser {
	
	private final static Logger logger = Logger.getLogger(TimeParser.class);
	private final static String DEFAULT_FORMAT = "HH:mm:ss.S";
	final SimpleDateFormat sdf;
	
	public TimeParser(String simpleDateFormat) {
		sdf = (simpleDateFormat==null || simpleDateFormat.isEmpty()) ? new SimpleDateFormat(DEFAULT_FORMAT) : new SimpleDateFormat(simpleDateFormat);
	}

	public Object parse(String token) {
		if(token.length()>0){
			try{
				return new Time(sdf.parse(token).getTime());
			}catch(Exception e){
				logger.warn(e.getMessage());
				logger.warn(String.format("Error parsing token %s. Pushing null instead.", token));
				return null;
			}
		}
		return null;
	}

}
