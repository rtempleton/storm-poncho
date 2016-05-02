package com.github.rtempleton.poncho.io.parsers;

public class StringParser implements TokenParser {

	public Object parse(String token) {
		return token;
	}
	
	public Object parse(char[] input, int start, int len){
		return new String(input, start, len);
		
	}

}
