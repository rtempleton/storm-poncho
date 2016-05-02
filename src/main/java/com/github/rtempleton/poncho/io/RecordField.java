package com.github.rtempleton.poncho.io;

import com.github.rtempleton.poncho.io.parsers.DateParser;
import com.github.rtempleton.poncho.io.parsers.DoubleParser;
import com.github.rtempleton.poncho.io.parsers.FloatParser;
import com.github.rtempleton.poncho.io.parsers.IntParser;
import com.github.rtempleton.poncho.io.parsers.LongParser;
import com.github.rtempleton.poncho.io.parsers.StringParser;
import com.github.rtempleton.poncho.io.parsers.TimeParser;
import com.github.rtempleton.poncho.io.parsers.TimestampParser;
import com.github.rtempleton.poncho.io.parsers.TokenParser;

public class RecordField{

	public String name;
	public TokenType type;
	public String format;
	
	public enum TokenType{
		STRING, INTEGER, LONG, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP
	}
	
	public RecordField(){
		
	}
	
	public TokenParser getParser(){
		return createParser(type, format);
	}
	
	
	private static TokenParser createParser(TokenType type, String format){
		switch (type){
		
		case STRING :
			return new StringParser();
			
		case INTEGER :
			return new IntParser(format);
			
		case LONG :
			return new LongParser(format);
		
		case FLOAT :
			return new FloatParser(format);
			
		case DOUBLE :
			return new DoubleParser(format);
			
		case TIMESTAMP :
			return new TimestampParser(format);
			
		case DATE :
			return new DateParser(format);
			
		case TIME :
			return new TimeParser(format);
		}
		
		
		return null;
	}
	
	
	
	
	
}
