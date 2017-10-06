package com.github.rtempleton.poncho.io;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.test.AbstractTestCase;

public class TestParseDelimitedTextBolt extends AbstractTestCase {
	
	private static final Logger logger = LoggerFactory
			.getLogger(TestParseDelimitedTextBolt.class);
	
	private static final String SCHEMA_PROPERTY = "DTS.schema";
	private static final String DELIMITER_PROPERTY = "DTS.delimiter";
	private static final String FILTER_FIELDS = "DTS.FilterFields";
	
	@Test
	public void testDelimitedTextScheme() throws JsonParseException, JsonMappingException, IOException{
		
		logger.info("###testDelimitedScheme###");
		String schemaDefinitionPath = getResourcePath("TestSchema.json");
		String delimiter = "\\t";
		String testFilePath = getResourcePath("TestFile.txt");
		
		Properties props = new Properties();
		props.put(SCHEMA_PROPERTY, schemaDefinitionPath);
		props.put(DELIMITER_PROPERTY, delimiter);
		
		ParseDelimitedTextBolt b = new ParseDelimitedTextBolt(props, Arrays.asList("value"));
		b.prepare(null, null, null);
		
		
		BufferedReader br = new BufferedReader(new FileReader(testFilePath));
		for (String line; (line = br.readLine()) != null;) {
			StringBuffer buf = new StringBuffer();
			try{
				List<Object> parsed = b.doProcess(line);
				for (Object o : parsed){
					buf.append(o + " ");
				}
				logger.info(buf.toString());
			}catch(Exception e){
				logger.error("parsing error - skip message");
			}
			
			
		}
		br.close();
	}
	
	
	@Test
	public void testSelectFields() throws JsonParseException, JsonMappingException, IOException{
		
		logger.info("###testSelectFields###");
		String schemaDefinitionPath = getResourcePath("TestSchema.json");
		String delimiter = "\\t";
		String filterFields = "DoubleCol2";
		String testFilePath = getResourcePath("TestFile.txt");
		
//		String schemaDefinitionPath = "/Users/rtempleton/Documents/workspace/CDR_Storm/src/test/resources/com/github/rtempleton/cdr_storm/cdr_schema.json";
//		String delimiter = ";";
//		String filterFields = "Call_status,CPN,CN,DN,Addr_Nature,I_tg_id,I_iam_t,I_acm_t,R_lrn,I_rel_t,I_rel_cause,E_tg_id,E_setup_t,E_acm_t,E_rel_t,E_rel_cause,I_jip,Early_events,Call_duration_cust";
//		String testFilePath = "/Users/rtempleton/Documents/workspace/CDR_Storm/src/test/resources/com/github/rtempleton/cdr_storm/CDRSample.txt";
		
		
		Properties props = new Properties();
		props.put(SCHEMA_PROPERTY, schemaDefinitionPath);
		props.put(DELIMITER_PROPERTY, delimiter);
		props.put(FILTER_FIELDS, filterFields);
		
		ParseDelimitedTextBolt b = new ParseDelimitedTextBolt(props, Arrays.asList("value"));
		b.prepare(null, null, null);
		
		BufferedReader br = new BufferedReader(new FileReader(testFilePath));
		for (String line; (line = br.readLine()) != null;) {
			StringBuffer buf = new StringBuffer();
			try{
				List<Object> parsed = b.doProcess(line);
				for (Object o : parsed){
					buf.append(o + " ");
				}
				logger.info(buf.toString());
			}catch(Exception e){
				logger.error("parsing error - skip message");
			}
		}
		br.close();
	}
	
}
