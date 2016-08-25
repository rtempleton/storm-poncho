package com.github.rtempleton.poncho;

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

public class SelectFieldsBolt implements IRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(SelectFieldsBolt.class);
	private List<String> outputFields;
	private OutputCollector collector;
	private int[] inputPositions;
	
	public enum SelectType {REMOVE, RETAIN};
	
	/**
	 * Bolt used to control which fields will be emitted in the output Tuple.
	 * 
	 * @param props
	 * @param inputFields - the fields in the Tuple that feeds this bolt
	 * @param type - SelectFieldsBot.SelectType - REMOVE: Removes from the inputFields list, the fields specified in the selectFields list. RETAIN: Retains fromt he inputFields list, the fields specified in the selectFields list.
	 * @param selectFields - the list of fields you wish to REMOVE or RETAIN from the input fields.
	 */
	public SelectFieldsBolt(Properties props, final List<String> inputFields, SelectType type, List<String> selectFields){
		
		inputPositions = new int[selectFields.size()];
		
		if(type.equals(SelectType.REMOVE)){
			inputFields.removeAll(selectFields);
			this.outputFields = inputFields;
			inputPositions = new int[outputFields.size()];
			//map the positions of the input fields based on the fields from the output schema.
			for (int i=0;i<outputFields.size();i++){
				if(inputFields.indexOf(outputFields.get(i))!=-1){
					inputPositions[i]=inputFields.indexOf(outputFields.get(i));
				}else{
					logger.error(String.format("The selectField value %s was not found in the inputFields list", selectFields.get(i)));
					System.exit(-1);
				}
			}	
			
		}else{
			if(inputFields.containsAll(selectFields))
			this.outputFields = selectFields;
			inputPositions = new int[selectFields.size()];
			//map the positions of the input fields based on the fields from the select schema. This is different from the above implementation in order to respect the order of the selectFields.
			for (int i=0;i<selectFields.size();i++){
				if(inputFields.indexOf(selectFields.get(i))!=-1){
					inputPositions[i]=inputFields.indexOf(selectFields.get(i));
				}else{
					logger.error(String.format("The selectField value %s was not found in the inputFields list", selectFields.get(i)));
					System.exit(-1);
				}
			}
		}
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		List<Object> vals = new ArrayList<Object>();
		for(int i=0;i<outputFields.size();i++){
			vals.add(input.getValue(inputPositions[i]));
		}
		
		collector.emit(input, vals);
		collector.ack(input);
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputFields));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
