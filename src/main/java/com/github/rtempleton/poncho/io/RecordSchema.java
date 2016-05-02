package com.github.rtempleton.poncho.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

public class RecordSchema{
	
	private static final Logger logger = Logger.getLogger(RecordSchema.class);
	
	final List<RecordField> fields;
	
	public RecordSchema(List<RecordField> fields){
		this.fields = fields;
	}
	
	public int getFieldCount(){
		return fields.size();
	}
	
	public List<String> getFieldNames(){
		ArrayList<String> fieldNames = new ArrayList<String>(fields.size()); 
		for(RecordField fld : fields){
			fieldNames.add(fld.name);
		}
		return fieldNames;
	}
	
	int getFieldPos(String fieldName){
		int pos = -1;
		for(int i=0;i<fields.size();i++){
			if(fieldName.equals(fields.get(i).name))
				return i;
		}
		return pos;
	}
	

}
