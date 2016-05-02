package com.github.rtempleton.poncho.io.parsers;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

public class DateParser implements TokenParser {
	
	private final static Logger logger = Logger.getLogger(DateParser.class);
	private final static String DEFAULT_FORMAT = "yyyy-MM-dd";
	final SimpleDateFormat sdf;
	
	public DateParser(String simpleDateFormat){
		sdf = (simpleDateFormat==null || simpleDateFormat.isEmpty()) ? new SimpleDateFormat(DEFAULT_FORMAT) : new SimpleDateFormat(simpleDateFormat);
	}

	public Object parse(String token) {
		try {
			return sdf.parse(token);
		} catch (ParseException e) {
			logger.warn(e.getMessage());
			logger.warn(String.format("Error parsing token %s. Pushing null instead.", token));
			return null;
		}
	}

}
