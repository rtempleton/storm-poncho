package com.github.rtempleton.poncho;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


public class TextFileProducer {
	
	private KafkaProducer<String, String> producer;
	private static final Logger logger = Logger
			.getLogger(TextFileProducer.class);

	private final String sourceFile;
	private final String topic;
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			USAGE();
		}
		
		TextFileProducer prod = new TextFileProducer(args[0], args[1]);
		prod.run();
	}
	
	/**
	 * 
	 * @param propsFile
	 * @param sourceFile
	 */
	public TextFileProducer(String propsFile, String sourceFile){
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(propsFile));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		topic = StormUtils.getRequiredProperty(props, "kafka.topic");
		this.sourceFile = sourceFile;
		producer = new KafkaProducer<String, String>(props);		
	}
	
	static void USAGE() {
		logger.error("TestFileProducer requires two arguments to run:");
		logger.error("\t Path to a configuration file for Kafka");
		logger.error("\t Path to a source file to be written to Kafka");
//		logger.error("\t (optional) the max number of records to be loaded");
		System.exit(-1);
	}
	
	
	/**
	 * Executes the producer to load data from the source file to the Kafaka queue specified in the configuration
	 */
	public void run() {
		int cntr = 0;
		
		try  {
			BufferedReader br = new BufferedReader(new FileReader(sourceFile));
			for (String line; (line = br.readLine()) != null;) {
				
				producer.send(new ProducerRecord<String, String>(topic, line));
				cntr++;
				if(cntr%1000==0){
					logger.info(String.format("Wrote %d records", cntr));
				}

			}
			
			br.close();
			producer.close();
			logger.info(String.format("Finished!! Wrote %d total records.",
					cntr));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
