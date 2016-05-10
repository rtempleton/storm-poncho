package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;

import org.apache.log4j.Logger;

public class DoubleParser implements TokenParser {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DoubleParser.class);
	private final DecimalFormat df;
	private final String name;

	/**
	 * 
	 * @param decimalFormat
	 *            - An optional {@link DecimalFormat} pattern
	 */
	public DoubleParser(String name, String decimalFormat) {
		df = (decimalFormat == null || decimalFormat.isEmpty()) ? new DecimalFormat()
				: new DecimalFormat(decimalFormat);
		this.name=name;
	}

	public Object parse(String token) {
		try {
			return (Double)df.parse(token).doubleValue();
		} catch (Exception e) {
			logger.warn(e.getMessage());
			logger.warn(String.format("Error parsing token %s at field %s. Pushing null instead.", token, name));
			return null;
		}
	}

}
