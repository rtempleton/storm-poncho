package com.github.rtempleton.poncho.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.StormUtils;

/**
 * 
 * @author rtempleton
 * 
 * Storm bolt for upserting records into Phoenix tables
 * 
 * Expected properties:
 * .jdbcurl - The JDBC connection string. Assumes you will be communicating using the thick client driver.
 * .table - The name of the Phoenix table where you will be inserting data
 *
 */
public class PhoenixUpsertBolt implements IRichBolt {


	private static final long serialVersionUID = 1L;
	private static final Logger Log = LoggerFactory.getLogger(PhoenixUpsertBolt.class);
	
	private final List<String> inputFields;
	private final long TICK_TUPLE_SECS;
	private final int BATCH_SIZE;
	private final String table;
	private final String jdbcurl;
	private final ArrayList<Tuple> cache;
	
	private Connection con = null;
	private String upsertStmt = "";
	private final ArrayList<String> cols = new ArrayList<String>();
	
	private OutputCollector collector;

	public PhoenixUpsertBolt(Properties props, String boltName, List<String> inputFields) {
		this.inputFields = inputFields;
		jdbcurl = StormUtils.getRequiredProperty(props, boltName + ".jdbcUrl");
		table = StormUtils.getRequiredProperty(props, boltName + ".table");
		TICK_TUPLE_SECS = (props.getProperty("phoenix.writerFlushFreqSecs")!=null)?Long.parseLong(props.getProperty("phoenix.writerFlushFreqSecs")):10l;
		BATCH_SIZE = (props.getProperty("phoenix.flushBatchSize")!=null)?Integer.parseInt(props.getProperty("phoenix.flushBatchSize")):300;
		cache = new ArrayList<Tuple>(BATCH_SIZE);
	}

	@Override
	public void cleanup() {
		try {
			if(con!=null && !con.isClosed())
				con.close();
		}catch (SQLException e){
			
		}
	}

	@Override
	public void execute(Tuple input) {
		
		if (isTickTuple(input)){
			if (!cache.isEmpty())
				flushCache();
		}else{
			cache.add(input);
			if(cache.size()==BATCH_SIZE){
				flushCache();
			}
		}
			
		collector.ack(input);
	}
	
	
	private static boolean isTickTuple(Tuple tuple) {
	    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
	        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
	
	private void flushCache(){
		try{
			con = DriverManager.getConnection(jdbcurl);
			con.setAutoCommit(false);
			PreparedStatement stmt = con.prepareStatement(upsertStmt);
			
			//iteratore over the tuples in the cache mapping the fieldNames to the column names in the table
			for (Tuple input : cache){
				int i=1;
				stmt.clearParameters();
				for(String key : cols) {
					stmt.setObject(i++, input.getValueByField(key));
				}
				stmt.addBatch();
			}
			
			stmt.executeBatch();
			con.commit();
			stmt.close();
			con.close();
			
			//pass the cached inputs through on the "default" output stream
			for(Tuple input : cache) {
				collector.emit(input.getValues());
			}
			
		}catch(SQLException e) {
			//if there's an exception, dump the cache conectents into the "failed" output stream
			Log.error("Error UPSERTING batch of " + cache.size() + " records. " + e.getMessage());
			for(Tuple input : cache) {
				collector.emit("failed", input.getValues());
			}
		}finally{
			cache.clear();
		}
		
	}
	
	

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		Log.info("Initializing: " + this.getClass().getName());
		String mdQuery = "select * from " + this.table + " where 1=0";
		
		PreparedStatement stmt = null;
		ResultSet rset = null;
		
		//cache the table metadata
		try {
			con = DriverManager.getConnection(jdbcurl);
			stmt = con.prepareStatement(mdQuery);
			rset = stmt.executeQuery();
			ResultSetMetaData meta = rset.getMetaData();
			for (int i=1; i<meta.getColumnCount()+1; i++) {
				cols.add(meta.getColumnName(i));
			}
		} catch (SQLException e) {
			Log.error("Error retrieving table meta data: " + e.getMessage());
			System.exit(-1);
		}finally {
			try {
				rset.close();
				stmt.close();
				con.close();
			}catch(Exception e) {
				Log.error("There was an error closing resources during the initialization");
			}
		}
		
		//create the upsert statement for this table
		StringBuffer buf = new StringBuffer();
		buf.append("upsert into " +  table.toUpperCase() + " values (");
		for(int i=0;i<cols.size();i++) {
			buf.append("?,");
			}
		//drop the last comma from the params list
		buf.deleteCharAt(buf.length()-1);
		buf.append(")");
		upsertStmt = buf.toString();
	}

	/*
	 * Sets the "default" output schema and the "failed" output schema
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		final ArrayList<String> outputFields = new ArrayList<String>();
		outputFields.addAll(inputFields);
		declarer.declare(new Fields(outputFields));
		declarer.declareStream("failed", new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// configure how often a tick tuple will be sent to our bolt
	    Config conf = new Config();
	    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_TUPLE_SECS);
	    return conf;
	}

}
