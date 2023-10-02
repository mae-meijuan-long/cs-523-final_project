package cs523.SparkStreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.LoggerFactory;

import cs523.Constants;
import scala.Tuple2;
public class Kafka2stream2HBase{

	 static org.slf4j.Logger logger = LoggerFactory.getLogger(Kafka2stream2HBase.class.getName());
	 private static final Pattern SPACE = Pattern.compile(",");
	 
	 static String brokers  ;
	
	public static void main(String[] args) throws Exception {
		    if (args.length < 1) {
		      System.err.println("Usage: Kafka2stream2HBase <groupId>\n" +
		                         "  <groupId> is a consumer group name to consume from topics\n"+"\n");
		      System.exit(1);
		    }

		    String groupId = args[0];

		    SparkConf sparkConf = new SparkConf().setAppName("cs-523-spark-consume-kafka-2-hbase")
		    		.setMaster("local[2]");//only use in local mode
		    
		    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		    jssc.sparkContext().setLogLevel("INFO");
		    
		    
		   
		    Set<String> topicsSet = new HashSet<>(Arrays.asList(Constants.KAFKA_TOPIC_NAME.split(",")));
		    Map<String, Object> kafkaParams = new HashMap<>();
		    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BOOTSTRAP_SERVER_VALUE);
		    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		    
		    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
		            jssc,
		            LocationStrategies.PreferConsistent(),
		            ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
		    
		    
		    messages.foreachRDD(f->{
		    	 JavaPairRDD<String, List<String>> formattedRdd = f.map(ConsumerRecord::value).filter(x->x!=null).mapToPair(line->
		    	 {
		    		 List<String> al = Arrays.asList(SPACE.split(line)); 
		    		 logger.info("prepared data=====>"+al.toString());
		    		 return new Tuple2<String, List<String>>(line,al);
		    	});
		    	 long totalCnt = formattedRdd.count();
		    	 if(totalCnt>0) {
		    		 formattedRdd.cache();
			    	 long lowQualityRecCnt = formattedRdd.filter(x->x._2.size()!=5 || x._2.get(1).trim().equals(Constants.INVALID_VALUE)).count();
			    	 logger.info("batch lowQualityRecCnt======>"+lowQualityRecCnt);
			    	 
			    	 formattedRdd.filter(x->x._2.size()==5 && !x._2.get(1).trim().equals(Constants.INVALID_VALUE)).foreachPartition( outLine->{
			    	 Configuration config = HBaseConfiguration.create();
				     config.set("hbase.zookeeper.quorum", Constants.HBASE_ZK_QUORUM);
				     
				     Connection connection = null  ;
				     Table table = null  ;
			    	 try {
			    	      connection =  ConnectionFactory.createConnection(config);
			    	      table = connection.getTable(TableName.valueOf(Constants.TARGET_HBASE_TABLE));
			    	      while(outLine.hasNext()){
			    	    	  Tuple2<String, List<String>>  eachLine =   outLine.next();
			    	    	  Put put = new Put(Bytes.toBytes(eachLine._1));
				    	      put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("state"), Bytes.toBytes(eachLine._2.get(0)));
				    	      put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("record_note"), Bytes.toBytes(eachLine._2.get(1)));
				    	      put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("count"), Bytes.toBytes(eachLine._2.get(2)));
				    	      put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("percent"), Bytes.toBytes(eachLine._2.get(3)));
				    	      put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("metric"), Bytes.toBytes(eachLine._2.get(4)));
				    	      table.put(put);
			    	      }
			    	      logger.info("===========data inserted successfully============");
			    	    } catch (Exception e) {
			    	      e.printStackTrace();
			    	    }finally {
			    	        //close connections 
			    	        if (table != null) table.close();
			    	        if (connection != null) connection.close();
			    	      }
			    	 });
			    	 formattedRdd.unpersist();
		    	 }
		    	 
		     });
		     
		      
		    jssc.start();
		    jssc.awaitTermination();
	}
	
}
