package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;
import java.text.ParseException;

import org.apache.log4j.Logger;

public class FloatParser implements TokenParser {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(FloatParser.class);
	private final DecimalFormat df;
	private final String name;
	private Object nullValue;

	/**
	 * 
	 * @param decimalFormat
	 *            - An optional {@link DecimalFormat} pattern
	 */
	public FloatParser(String name, String decimalFormat, String nullVal) {
		df = (decimalFormat == null || decimalFormat.isEmpty()) ? new DecimalFormat()
				: new DecimalFormat(decimalFormat);
		try {
			nullValue = (nullVal == null || nullVal.isEmpty()) ? null : (Float) df.parse(nullVal.trim()).floatValue();
		} catch (ParseException e) {
			nullValue = null;
		}
		this.name=name;
	}

	public Object parse(String token) {
		try {
			if(token.trim().length()==0)
				return nullValue;
			return (Float) df.parse(token.trim()).floatValue();
		} catch (Exception e) {
			logger.debug(String.format("Error parsing token \"%s\" at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
	}

}
