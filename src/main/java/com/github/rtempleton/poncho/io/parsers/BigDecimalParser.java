package com.github.rtempleton.poncho.io.parsers;

import java.math.BigDecimal;
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigDecimalParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(BigDecimalParser.class);
	private final String name;
	private final Object nullValue;
	
	/**
	 * 
	 * @param decimalFormat - An optional {@link DecimalFormat} pattern
	 */
	public BigDecimalParser(String name, Object nullVal) {
		this.name=name;
		if(nullVal == null)
			nullValue = null;
		else if (!(nullVal instanceof java.math.BigDecimal))
			nullValue = parse(nullVal);
		else
			nullValue = nullVal;
		
	}

	public Object parse(Object token) {
		
		if(token==null)
			return nullValue;
		
		try{
			if (!(token instanceof java.math.BigDecimal)) {
				String t = token.toString();
				if(t.trim().length()==0)
					return nullValue;
				return new BigDecimal(t);
			}else {
				return token;
			}
			
			
		}catch(Exception e){
			logger.debug(String.format("Error parsing token %s at field %s. Pushing %s instead.", token, name, nullValue));
			return nullValue;
		}
	}

}
