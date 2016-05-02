package com.github.rtempleton.poncho.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import com.github.rtempleton.poncho.AbstractTestCase;
import com.github.rtempleton.poncho.io.DelimitedTextScheme;
import com.github.rtempleton.poncho.io.RecordSchema;
import com.github.rtempleton.poncho.io.SchemaUtil;

public class TestTextScheme extends AbstractTestCase {
	
	private static final Logger logger = Logger
			.getLogger(TestTextScheme.class);
	
	@Test
	public void testDelimitedTextScheme() throws JsonParseException, JsonMappingException, IOException{
		
		logger.info("###testDelimitedScheme###");
		String schemaDefinitionPath = getResourcePath("TestSchema.json");
		String delimiter = "\\|";
		String testFilePath = getResourcePath("TestFile.txt");
		
		RecordSchema schema = SchemaUtil.readRecordSchema(schemaDefinitionPath);
		DelimitedTextScheme scheme = new DelimitedTextScheme(schema, delimiter);
		
		BufferedReader br = new BufferedReader(new FileReader(testFilePath));
		for (String line; (line = br.readLine()) != null;) {
			List<Object> parsed = scheme.deserialize(line.getBytes());
			for (Object o : parsed){
				logger.info(o.getClass() + " " + o.toString());
			}
			
		}
		br.close();
	}
	
	
	@Test
	public void testSelectFields() throws JsonParseException, JsonMappingException, IOException{
		
		logger.info("###testSelectFields###");
		String filterFields = "intCol1,txCol7,stringCol3,floatCol5";
		String schemaDefinitionPath = getResourcePath("TestSchema.json");
		String delimiter = "\\|";
		String testFilePath = getResourcePath("TestFile.txt");
		
		RecordSchema schema = SchemaUtil.readRecordSchema(schemaDefinitionPath);
		DelimitedTextScheme scheme = new DelimitedTextScheme(schema, delimiter);
		scheme.setSelectedFields(SchemaUtil.parseSelectFields(filterFields));
	
		
		BufferedReader br = new BufferedReader(new FileReader(testFilePath));
		for (String line; (line = br.readLine()) != null;) {
			List<Object> parsed = scheme.deserialize(line.getBytes());
			for (Object o : parsed){
				logger.info(o.getClass() + " " + o.toString());
			}
			
		}
		br.close();
	}
	
}
