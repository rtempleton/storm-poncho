package com.github.rtempleton.poncho.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class SchemaUtil {
	
	private static final Logger logger = Logger.getLogger(SchemaUtil.class);
	
	
	public static RecordSchema readRecordSchema(String schemaPath){
		
		try{
			BufferedReader br = new BufferedReader(new FileReader(schemaPath));
			StringBuffer sb = new StringBuffer();
			for (String line; (line = br.readLine()) != null;) {
				sb.append(line);
			}
			br.close();
			
			ObjectMapper mapper = new ObjectMapper();
		
			List<RecordField> foo = mapper.readValue(sb.toString(), new TypeReference<List<RecordField>>() {});
			return new RecordSchema(foo);
		}catch(Exception e){
			logger.error("Error parsing schema");
    		e.printStackTrace();
    		System.exit(-1);
    		return null;
		}
	}
	
	/*
	 * Takes a comma delimited list of select fields and returns a List<String> of selectFields as required by
	 * the DelimtedTextScheme
	 */
	public static List<String> parseSelectFields(String selectFields){
		List<String> fields = new ArrayList<String>(Arrays.asList(selectFields.split(",")));
		for(String field : fields){
			field.trim();
		}
		return fields;
	}
	
	

}
