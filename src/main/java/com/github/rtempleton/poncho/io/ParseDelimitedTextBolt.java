package com.github.rtempleton.poncho.io;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.github.rtempleton.poncho.StormUtils;
import com.github.rtempleton.poncho.io.parsers.TokenParser;

public class ParseDelimitedTextBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(ParseDelimitedTextBolt.class);
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
	private final String charsetName;
	
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
	public ParseDelimitedTextBolt(Properties props){
		String schemaPath = StormUtils.getRequiredProperty(props, SCHEMA_PROPERTY);
		schema = SchemaUtil.readRecordSchema(schemaPath);
		delimiter = (props.containsKey(DELIMITER_PROPERTY)) ? props.getProperty(DELIMITER_PROPERTY) : DEFAULT_DELIMITER;
		selectedFields = (props.containsKey(FILTER_FIELDS)) ? SchemaUtil.parseSelectFields(props.getProperty(FILTER_FIELDS)) : schema.getFieldNames();
		charsetName = (props.containsKey(DEFAULT_CHARSET_PROPNAME)) ? props.getProperty(DEFAULT_CHARSET_PROPNAME) : DEFAULT_CHARSET_VALUE;
 	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		mapping.clear();
		parsers.clear();
		for(String fldName : selectedFields){
			int fldPos = schema.getFieldPos(fldName);
			if(fldPos >-1){
				mapping.add(fldPos);
				parsers.add(schema.fields.get(fldPos).getParser());
			}else{
				logger.warn(String.format("Field %s was not found in the record schema, this field will not be included.", fldName));
			}
		}

	}

	/**
	 * Receives the Tuple from the upstream KafkaBolt using the RawScheme implementation.
	 */
	public void execute(Tuple input) {
		try{
			String rec = new String(input.getBinary(0), charsetName);
			collector.emit(doProcess(rec));
		}catch(UnsupportedEncodingException e){
			//TODO - create stream for records that weren't able to be decoded and push bad tuples there for potential inspection
			logger.error("Parsing Error - Skipping Message");
			logger.error(e.getMessage());
		}
		collector.ack(input);
		
	}
	
	/**
	 * Callable by unit tests
	 * @param input
	 * @return
	 */
	protected Values doProcess(String input){
		Values vals = new Values();
		String[] rec = input.split(delimiter);
		for(int i=0;i<parsers.size();i++){
			//get the position of the input field from the mapping list
			int inPos = mapping.get(i);
			vals.add(parsers.get(i).parse(rec[inPos]));
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
