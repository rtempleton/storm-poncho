package com.github.rtempleton.poncho.io;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class ConsoleLoggerBolt implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(ConsoleLoggerBolt.class);
	private final List<String> inputFields;
	private boolean header = true;
	
	private OutputCollector collector;

	public ConsoleLoggerBolt(Properties props, final List<String> inputFields){
		this.inputFields = inputFields;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		printHeader();
	}

	public void execute(Tuple input) {
		
		if(header){
			printHeader();
			header = !header;
		}
		
		StringBuffer b = new StringBuffer();
		for (Object o : input.getValues()){
			String val = (o!=null) ? o.toString() : "";
			b.append(val + ", ");
		}
		logger.info(b.substring(0, b.length()-2));
		
		collector.ack(input);
		
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields(new ArrayList<String>()));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void printHeader(){
		StringBuffer b = new StringBuffer();
		for (String field : inputFields){
			b.append(field + ", ");
		}
		logger.info(b.substring(0, b.length()-2));
	}

}
