package com.github.rtempleton.poncho;

import org.junit.Test;

public class TestPoncho extends AbstractTestCase {
	
	private final String propsFile = getResourcePath() + "/../producer.properties";
	private final String cdrSampleFile = getResourcePath() + "/../CDRSample.txt";
	int recLimit = Integer.MAX_VALUE;
	
	
	@Test
	public void testKafkaLoader(){
		TextFileProducer producer = new TextFileProducer(propsFile, cdrSampleFile, recLimit);
		producer.run();
	}
	

}
