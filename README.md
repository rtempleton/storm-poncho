Poncho
===

Doing a fair bit of Storm project development I often found myself repeating the same tasks. Rather than continually cutting and pasting these boiler plate functions from previous projects, I figured it would be best to encapsulate these into it's own project **Poncho - As useful item to have in a storm!**

c.g.r.poncho.TextFileProducer
---
An externally callable utility to load data from a source text file line by line into Kafka. It's handy for unit tests.

c.g.r.poncho.io.ParseDelimitedTextBolt
---
I wanted to provide a way to externally define the schema of the records being read off the Kafka queue and pass that schema as a parameter to the Bolt that is doing the record parsing. From the schema, the bolt will understand the record layout and confiugre the underlying parsers to correctly interpret the record. This bolt will immediately follow a KafkaSpout which implements a "RawScheme"

Current supported data types are:

* String
* Integer, Long - take optional formatting arguments that align to [DecimalFormat](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html)
* Float, Double - take optional formatting arguments that align to [DecimalFormat](https://docs.oracle.com/javase/8/docs/api/java/text/DecimalFormat.html)
* Timestamp, Date, Time - take optional formatting arguments that align to [SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html). Are backed by Joda [DateTime](http://www.joda.org/joda-time/apidocs/org/joda/time/DateTime.html), [LocalDate](http://www.joda.org/joda-time/apidocs/org/joda/time/LocalDate.html) and [LocalTime](http://www.joda.org/joda-time/apidocs/org/joda/time/LocalTime.html) Classes respectively. 


sample schema

    [
        {"name":"intCol1", "type":"INTEGER"},
        {"name":"intCol2", "type":"INTEGER", "format":"###,###.###"},
        {"name":"stringCol3", "type":"STRING"},
        {"name":"floatCol4", "type":"FLOAT"},
        {"name":"floatCol5", "type":"FLOAT", "format":"###,###.###"},
        {"name":"tsCol6", "type":"TIMESTAMP"},
        {"name":"txCol7", "type":"TIMESTAMP", "format":"dd-MM-yyyy HH:mm:ss.S"}
    ]

Use this class in conjunction with the StormUtils.getKafkaSpout to add a KafkaSpout to your graph.

If you wish to limit the fields that appear in your output tuple, you can also pass an optional comma separated list of of those field names. This will also update the declared ouptut fields.

	public class MyTopology{
	
		private final Properties topologyConfig;
		
		public static void main(String[] args) throws Exception{
			MyTopology topo = new MyTopology(args);
			topo.submit(topo.compose());
		}
		
		public CDRTopology(String... args) throws Exception{
			...
			topologyConfig = new Properties();
			try {
				topologyConfig.load(new FileInputStream(args[0]));
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		protected TopologyBuilder compose() throws Exception{
		
			TopologyBuilder builder = new TopologyBuilder();
			
			IRichSpout kafkaSpout = StormUtils.getKafkaSpout(topologyConfig, new RawScheme());
		    builder.setSpout("spout", kafkaSpout);
		    
		    IRichBolt reader = new ParseDelimitedTextBolt(topologyConfig);
		    builder.setBolt("reader", reader).localOrShuffleGrouping("spout");
		
		    .........
			
		}
		
		protected void submit(TopologyBuilder builder){
		try {
			StormSubmitter.submitTopology("MyTopology", topologyConfig, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} 
	
	}
	
c.g.r.poncho.io.SchemaGenerator
---
Generates a boiler plate schema. Simply pass in the number of fields you will read in your DelimitedTextScheme and the full path to  where you want to the schema written.

c.g.r.poncho.io.SimpleHDFSWriter
---
Wrapper class for HdfsBolt that simplifies configuration. Handy for unit testing.
