package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;

import org.apache.log4j.Logger;

public class IntParser implements TokenParser {
	
	private static final Logger logger = Logger.getLogger(IntParser.class);
	final DecimalFormat df;
	
	/**
	 * 
	 * @param decimalFormat - An optional {@link DecimalFormat} pattern
	 */
	public IntParser(String decimalFormat) {
		df = (decimalFormat==null || decimalFormat.isEmpty()) ? new DecimalFormat() : new DecimalFormat(decimalFormat);
	}

	public Object parse(String token) {
		if(token.length()>0){
			try{
				return (Integer)df.parse(token).intValue();
			}catch(Exception e){
				logger.warn(e.getMessage());
				logger.warn(String.format("Error parsing token %s. Pushing null instead.", token));
				return null;
			}
		}
		return null;
	}

}
