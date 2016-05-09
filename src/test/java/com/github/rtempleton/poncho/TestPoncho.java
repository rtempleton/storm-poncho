package com.github.rtempleton.poncho;

import org.junit.Ignore;

public class TestPoncho extends AbstractTestCase {
	
	private final String propsFile = getResourcePath() + "/config.properties";
	private final String sampleFile = getResourcePath() + "/io/TestFile.txt";
	
	
	@Ignore
	public void testKafkaLoader(){
		TextFileProducer producer = new TextFileProducer(propsFile, sampleFile);
		producer.run();
	}
	

}
