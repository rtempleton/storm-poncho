package com.github.rtempleton.poncho.io.parsers;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BooleanParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(BooleanParser.class);
	private final String name;
	private final List<Object> trueValues;
	
	/**
	 * 
	 * @param decimalFormat - An optional {@link DecimalFormat} pattern
	 */
	public BooleanParser(String name, List<Object> trueValues) {
		this.name=name;
		
		if(trueValues == null || trueValues.isEmpty()) {
			this.trueValues = new ArrayList<Object>();
			this.trueValues.add(true);
			this.trueValues.add("true");
		} else
			this.trueValues = trueValues;
		
	}

	public Object parse(Object token) {
		
		if(token==null)
			return new Boolean(false);
		
		try {
		if (trueValues.contains(token))
			return new Boolean(true);
		else
			return new Boolean(false);
		}catch(Exception e){
			logger.debug(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, false));
			return new Boolean(false);
		}
	}

}
