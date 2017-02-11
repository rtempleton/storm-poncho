package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;
import java.text.ParseException;

import org.apache.log4j.Logger;

public class DoubleParser implements TokenParser {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DoubleParser.class);
	private final DecimalFormat df;
	private final String name;
	private Object nullValue;

	/**
	 * 
	 * @param decimalFormat
	 *            - An optional {@link DecimalFormat} pattern
	 */
	public DoubleParser(String name, String decimalFormat, String nullVal) {
		df = (decimalFormat == null || decimalFormat.isEmpty()) ? new DecimalFormat()
				: new DecimalFormat(decimalFormat);
		try {
			nullValue = (nullVal == null || nullVal.isEmpty()) ? null : (Long)df.parse(nullVal.trim()).longValue();
		} catch (ParseException e) {
			nullValue = null;
		}
		this.name=name;
	}

	public Object parse(String token) {
		try {
			if(token.trim().length()==0)
				return nullValue;
			return (Double)df.parse(token).doubleValue();
		} catch (Exception e) {
			logger.debug(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
	}

}
