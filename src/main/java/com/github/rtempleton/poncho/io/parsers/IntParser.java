package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;

import org.apache.log4j.Logger;

public class IntParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(IntParser.class);
	private final DecimalFormat df;
	private final String name;
	
	/**
	 * 
	 * @param decimalFormat - An optional {@link DecimalFormat} pattern
	 */
	public IntParser(String name, String decimalFormat) {
		df = (decimalFormat==null || decimalFormat.isEmpty()) ? new DecimalFormat() : new DecimalFormat(decimalFormat);
		this.name=name;
	}

	public Object parse(String token) {
		if(token.length()>0){
			try{
				return (Integer)df.parse(token.trim()).intValue();
			}catch(Exception e){
				logger.warn(e.getMessage());
				logger.warn(String.format("Error parsing token %s at field %s. Pushing null instead.", token, name));
				return null;
			}
		}
		return null;
	}

}
