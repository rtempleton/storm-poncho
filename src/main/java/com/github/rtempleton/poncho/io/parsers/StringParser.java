package com.github.rtempleton.poncho.io.parsers;

public class StringParser implements TokenParser {
	
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private final String name;
	
	public StringParser(String name){
		this.name=name;
	}

	public Object parse(String token) {
		return token;
	}

}
