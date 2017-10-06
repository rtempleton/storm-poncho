package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShortParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(ShortParser.class);
	private final DecimalFormat df;
	private final String name;
	private final Object nullValue;
	
	/**
	 * 
	 * @param decimalFormat - An optional {@link DecimalFormat} pattern
	 */
	public ShortParser(String name, String decimalFormat, Object nullVal) {
		this.name=name;
		df = (decimalFormat==null || decimalFormat.isEmpty()) ? new DecimalFormat() : new DecimalFormat(decimalFormat);
		if(nullVal == null)
			nullValue = null;
		else if (!(nullVal instanceof java.lang.Short))
			nullValue = parse(nullVal);
		else
			nullValue = nullVal;
		
	}

	public Object parse(Object token) {
		
		if(token==null)
			return nullValue;
		
		try{
			if (!(token instanceof java.lang.Short)) {
				String t = token.toString();
				if(t.trim().length()==0)
					return nullValue;
				return (Short)df.parse(t.trim()).shortValue();
			}else {
				return token;
			}
			
			
		}catch(Exception e){
			logger.debug(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
	}

}
