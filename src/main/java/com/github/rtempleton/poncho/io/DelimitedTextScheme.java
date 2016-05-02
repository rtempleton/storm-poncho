package com.github.rtempleton.poncho.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.storm.shade.com.fasterxml.jackson.core.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.github.rtempleton.poncho.io.parsers.TokenParser;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DelimitedTextScheme implements Scheme {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DelimitedTextScheme.class);
	private final static String DEFAULT_DELIMITER = ",";
	
	private final List<Integer> mapping = new ArrayList<Integer>();
	private RecordSchema schema;
	private ArrayList<TokenParser> parsers = new ArrayList<TokenParser>();
	
	
	private final String delimiter;
	
	
	public DelimitedTextScheme (RecordSchema schema, String delimiter) throws JsonParseException, JsonMappingException, IOException{
		this.schema = schema;
		this.delimiter = delimiter;
		setSelectedFields(schema.getFieldNames());
		
	}
	
	
	public DelimitedTextScheme (RecordSchema schema) throws JsonParseException, JsonMappingException, IOException{
		this(schema, DEFAULT_DELIMITER);
	}
	
	/*
	 * Sets the output fields to match the provided list of fields. 
	 */
	public void setSelectedFields(List<String> selectFields){
		mapping.clear();
		parsers.clear();
		for(String fldName : selectFields){
			int fldPos = schema.getFieldPos(fldName);
			if(fldPos >-1){
				mapping.add(fldPos);
				parsers.add(schema.fields.get(fldPos).getParser());
			}else{
				logger.warn(String.format("Field %s was not found in the record schema, this field will not be included.", fldName));
			}
		}
	}
	

    public List<Object> deserialize(byte[] bytes) {
    	Values vals = new Values();
    	try {    		
    		String[] input = new String(bytes, "UTF-8").split(delimiter);
	    	for(int i=0;i<parsers.size();i++){
	    		//get the position of the input field from the mapping list
	    		int inPos = mapping.get(i);
	    		vals.add(parsers.get(i).parse(input[inPos]));
	    	}
    	}catch(UnsupportedEncodingException e){
    		logger.error("Parsing Error - Skipping Message");
			logger.error(e.getMessage());
    	}
    	
    	return vals;
    }

    
    public Fields getOutputFields() {
    	List<String> outputFields = new ArrayList<String>(mapping.size());
    	for(int i : mapping){
    		outputFields.add(schema.fields.get(i).name);
    	}
    	return new Fields(outputFields);
    }
    

}