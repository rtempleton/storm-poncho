package com.github.rtempleton.poncho.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordSchema implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(RecordSchema.class);
	
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
			fieldNames.add(fld.getName());
		}
		return fieldNames;
	}
	
	int getFieldPos(String fieldName){
		int pos = -1;
		for(int i=0;i<fields.size();i++){
			if(fieldName.equals(fields.get(i).getName()))
				return i;
		}
		return pos;
	}
	

}
