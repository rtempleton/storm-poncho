package com.github.rtempleton.poncho.io;

import java.io.Serializable;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

import com.github.rtempleton.poncho.io.parsers.DateParser;
import com.github.rtempleton.poncho.io.parsers.DoubleParser;
import com.github.rtempleton.poncho.io.parsers.FloatParser;
import com.github.rtempleton.poncho.io.parsers.IntParser;
import com.github.rtempleton.poncho.io.parsers.LongParser;
import com.github.rtempleton.poncho.io.parsers.StringParser;
import com.github.rtempleton.poncho.io.parsers.TimeParser;
import com.github.rtempleton.poncho.io.parsers.TimestampParser;
import com.github.rtempleton.poncho.io.parsers.TokenParser;

@JsonPropertyOrder({ "name", "type", "format", "nullVal" })
public class RecordField implements Serializable{

	private static final long serialVersionUID = 1L;
	private String name;
	private TokenType type;
	private String format;
	private String nullVal;
	
	public enum TokenType{
		STRING, INTEGER, LONG, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP
	}
	
	/**
	 * Used by SchemaGenerator
	 * @param name
	 * @param type
	 */
	protected RecordField(String name, TokenType type, String format, String nullVal){
		this.name=name;
		this.type=type;
		this.format=format;
		this.nullVal=nullVal;
	}
	
	public RecordField(){
	}
	
	
	@JsonIgnore
	public TokenParser getParser(){
		return createParser(name, type, format, nullVal);
	}
	
	
	private static TokenParser createParser(String name, TokenType type, String format, String nullVal){
		switch (type){
		
		case STRING :
			return new StringParser(name, nullVal);
			
		case INTEGER :
			return new IntParser(name, format, nullVal);
			
		case LONG :
			return new LongParser(name, format, nullVal);
		
		case FLOAT :
			return new FloatParser(name, format, nullVal);
			
		case DOUBLE :
			return new DoubleParser(name, format, nullVal);
			
		case TIMESTAMP :
			return new TimestampParser(name, format, nullVal);
			
		case DATE :
			return new DateParser(name, format, nullVal);
			
		case TIME :
			return new TimeParser(name, format, nullVal);
		}
		return null;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public TokenType getType() {
		return type;
	}

	public void setType(TokenType type) {
		this.type = type;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}
	
	public String getNullVal(){
		return this.nullVal;
	}
	
	public void setNullVal(String nullVal){
		this.nullVal=nullVal;
	}
	
	
	
	
	
}
