package com.github.rtempleton.poncho.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.github.rtempleton.poncho.io.RecordField.TokenType;

public class SchemaGenerator {
	
	private static final Logger logger = LoggerFactory.getLogger(SchemaGenerator.class);

	public static void main(String[] args) {
		if (args.length < 2){
			usage();
		}
		SchemaGenerator sg = new SchemaGenerator();
		try{
			int size = Integer.parseInt(args[0]);
			String path = args[1];
			sg.generateDelimitedTextSchema(size, path);
		}catch(Exception e){
			logger.error(e.getMessage());
			usage();
		}

	}
	
	
	protected void generateDelimitedTextSchema(int size, String path) throws JsonGenerationException, JsonMappingException, IOException{
		List<RecordField> fields = new ArrayList<RecordField>(size);
		for(int i=0; i<size; i++){
			fields.add(new RecordField("col" + (i+1), TokenType.STRING, "", ""));
		}
		
		ObjectMapper mapper = new ObjectMapper();
		mapper.writerWithDefaultPrettyPrinter().writeValue(new File(path), fields);
		
	}
	
	private static void usage(){
		logger.error("Schema Generator: Utility for creating a boilerplate Delimted Text Schema.\nArgument 1: number of fields to generate.\nArgument 2: path to the outputfile. If a file already exists in that location. You must delete it first.");
		System.exit(-1);
	}

}
