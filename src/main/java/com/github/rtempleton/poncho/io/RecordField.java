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

@JsonPropertyOrder({ "name", "type", "format" })
public class RecordField implements Serializable{

	private static final long serialVersionUID = 1L;
	public String name;
	public TokenType type;
	public String format;
	
	public enum TokenType{
		STRING, INTEGER, LONG, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP
	}
	
	/**
	 * Used by SchemaGenerator
	 * @param name
	 * @param type
	 */
	protected RecordField(String name, TokenType type, String format){
		this.name=name;
		this.type=type;
		this.format=format;
	}
	
	public RecordField(){
	}
	
	
	@JsonIgnore
	public TokenParser getParser(){
		return createParser(name, type, format);
	}
	
	
	private static TokenParser createParser(String name, TokenType type, String format){
		switch (type){
		
		case STRING :
			return new StringParser(name);
			
		case INTEGER :
			return new IntParser(name, format);
			
		case LONG :
			return new LongParser(name, format);
		
		case FLOAT :
			return new FloatParser(name, format);
			
		case DOUBLE :
			return new DoubleParser(name, format);
			
		case TIMESTAMP :
			return new TimestampParser(name, format);
			
		case DATE :
			return new DateParser(name, format);
			
		case TIME :
			return new TimeParser(name, format);
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
	
	
	
	
	
}
