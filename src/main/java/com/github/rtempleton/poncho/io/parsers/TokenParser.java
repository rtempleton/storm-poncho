package com.github.rtempleton.poncho.io.parsers;

import java.io.Serializable;

public interface TokenParser extends Serializable{
	
	public Object parse(String token);
	
}
