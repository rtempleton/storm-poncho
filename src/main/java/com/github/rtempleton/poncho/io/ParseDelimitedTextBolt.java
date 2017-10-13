package com.github.rtempleton.poncho.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.StormUtils;
import com.github.rtempleton.poncho.io.parsers.RecordSchema;
import com.github.rtempleton.poncho.io.parsers.TokenParser;
import com.github.rtempleton.poncho.io.utils.SchemaUtil;

public class ParseDelimitedTextBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger Log = LoggerFactory.getLogger(ParseDelimitedTextBolt.class);
	private static final String DEFAULT_DELIMITER = ",";
	private static final String DEFAULT_CHARSET_VALUE = "UTF-8";
	
	private static final String SCHEMA_PROPERTY = "DTS.schema";
	private static final String DELIMITER_PROPERTY = "DTS.delimiter";
	private static final String FILTER_FIELDS = "DTS.FilterFields";
	private static final String DEFAULT_CHARSET_PROPNAME = "DTS.charset_name";
	
	private final List<Integer> mapping = new ArrayList<Integer>();
	private final RecordSchema schema;
	private final ArrayList<TokenParser> parsers = new ArrayList<TokenParser>();
	private final List<String> selectedFields;
	private final String delimiter;
//	private final String charsetName;
	
	private int parseField = -1;
	
	private OutputCollector collector;

	/**
	 * Creates a series of underlying parsers as defined in the JSON document located at by the property DTS.schema. This Bolt should immediately follow the KafkaBolt which utilizes a RawSheme.
	 * <p>
	 * <b>REQUIRED PROPERTIES</b> that must be defined within the accompanying Properties argument
	 * <br><i>DTS.schema</i> - path to the JSON schema definition that describes the delimited text records on the Kafka topic.
	 * <p>
	 *  <b>OPTIONAL PROPERTIES</b>
	 *  <br><i>DTS.delimiter</i> - the field delimiter in the text records that will be read. Default value is comma ",".
	 *  <br><i>DTS.FilterFields</i> - command separated list of the field names described in the DTS.schema. Only this field names listed here will be output by KafkaBolt.
	 *  <br><i>DTS.charset_name</i> - The String name of the Charset used to decode the tuple byte[]. Uses "UTF-8" by default.
	 *  
	 * @param props - the Properties
	 */
	public ParseDelimitedTextBolt(Properties props, List<String> inputFields){
//		this.inputFields = inputFields;
		String schemaPath = StormUtils.getRequiredProperty(props, SCHEMA_PROPERTY);
		schema = SchemaUtil.readRecordSchema(schemaPath);
		delimiter = (props.containsKey(DELIMITER_PROPERTY)) ? props.getProperty(DELIMITER_PROPERTY) : DEFAULT_DELIMITER;
		selectedFields = (props.containsKey(FILTER_FIELDS)) ? SchemaUtil.parseSelectFields(props.getProperty(FILTER_FIELDS)) : schema.getFieldNames();
//		charsetName = (props.containsKey(DEFAULT_CHARSET_PROPNAME)) ? props.getProperty(DEFAULT_CHARSET_PROPNAME) : DEFAULT_CHARSET_VALUE;
		
		for(int i=0;i<inputFields.size();i++) {
			//hardcoding the name of the Kafka field here
			if(inputFields.get(i).equals("value"));
			parseField = i;
		}
		if (parseField==-1) {
			Log.error("The parse field was not found in the list of input fields");
		}
 	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		mapping.clear();
		parsers.clear();
		for(String fldName : selectedFields){
			int fldPos = schema.getFieldPos(fldName);
			if(fldPos >-1){
				mapping.add(fldPos);
				parsers.add(schema.getFields().get(fldPos).getParser());
			}else{
				Log.warn(String.format("Field %s was not found in the record schema, this field will not be included.", fldName));
			}
		}

	}

	/**
	 * Receives the Tuple from the upstream KafkaBolt using the RawScheme implementation.
	 */
	public void execute(Tuple input) {
		try{
			String rec = input.getString(parseField);
//			String rec = new String(input.getBinary(0), charsetName);
			collector.emit(doProcess(rec));
		}catch(Exception e){
			//TODO - create stream for records that weren't able to be decoded and push bad tuples there for potential inspection
			Log.error("Parsing Error - Skipping Message");
		}
		collector.ack(input);
		
	}
	
	/**
	 * Callable by unit tests
	 * @param input
	 * @return
	 */
	protected Values doProcess(String input) throws Exception{
		Values vals = new Values();
		
		String[] rec = input.split(delimiter);
		for(int i=0;i<parsers.size();i++){
			//get the position of the input field from the mapping list
			int inPos = mapping.get(i);
			try{
			vals.add(parsers.get(i).parse(rec[inPos]));
			}catch(Exception e){
				Log.error("Parse error source: " + input);
				Log.error("Parser: " + i);
				throw e;
			}
		}
		
		return vals;
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(selectedFields));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
