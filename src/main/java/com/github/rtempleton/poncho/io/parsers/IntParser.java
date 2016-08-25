package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;
import java.text.ParseException;

import org.apache.log4j.Logger;

public class IntParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(IntParser.class);
	private final DecimalFormat df;
	private final String name;
	private Object nullValue;
	
	/**
	 * 
	 * @param decimalFormat - An optional {@link DecimalFormat} pattern
	 */
	public IntParser(String name, String decimalFormat, String nullVal) {
		df = (decimalFormat==null || decimalFormat.isEmpty()) ? new DecimalFormat() : new DecimalFormat(decimalFormat);
		try {
			nullValue = (nullVal == null || nullVal.isEmpty()) ? null : (Integer)df.parse(nullVal.trim()).intValue();
		} catch (ParseException e) {
			nullValue = null;
		}
		this.name=name;
	}

	public Object parse(String token) {
		try{
			return (Integer)df.parse(token.trim()).intValue();
		}catch(Exception e){
			logger.warn(e.getMessage());
			logger.warn(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
	}

}
