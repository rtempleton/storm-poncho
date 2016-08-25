package com.github.rtempleton.poncho.io.parsers;

import org.apache.log4j.Logger;

public class StringParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private final static Logger logger = Logger.getLogger(StringParser.class);
	private final String name;
	private final Object nullValue;
	
	public StringParser(String name, String nullVal){
		this.name=name;
		nullValue = (nullVal == null || nullVal.isEmpty()) ? "" : nullVal;
	}

	public Object parse(String token) {
		if (token.trim().isEmpty()){
			logger.warn(String.format("Empty token at field %s. Pushing \"%s\" instead.", name, nullValue));
			return nullValue;
		}
		return token.trim();
	}

}
