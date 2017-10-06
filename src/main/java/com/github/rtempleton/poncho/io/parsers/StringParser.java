package com.github.rtempleton.poncho.io.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private final static Logger logger = LoggerFactory.getLogger(StringParser.class);
	private final String name;
	private final Object nullValue;
	
	public StringParser(String name, Object nullVal){
		this.name=name;
		this.nullValue = nullVal;
	}

	public Object parse(Object token) {
		
		if(token==null)
			return nullValue;
		
		if (!(token instanceof java.lang.String)) {
			String t = token.toString();
			if(t.trim().length()==0)
				return nullValue;
			return t.trim();
		}else {
			return token;
		}
		
//		if (token.trim().isEmpty()){
//			logger.debug(String.format("Empty token at field %s. Pushing \"%s\" instead.", name, nullValue));
//			return nullValue;
//		}

	}

}
