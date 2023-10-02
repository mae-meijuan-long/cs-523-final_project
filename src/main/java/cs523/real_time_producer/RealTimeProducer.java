package cs523.real_time_producer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs523.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RealTimeProducer {

    Logger logger = LoggerFactory.getLogger(RealTimeProducer.class.getName());

    Integer cnt = 1;
    public RealTimeProducer() {}

    public static void main(String[] args) throws InterruptedException, IOException {
    	URL url = RealTimeProducer.class.getResource("resources/");
    	String log4jConfPath = "log4j.properties";
    	PropertyConfigurator.configure(log4jConfPath);
        new RealTimeProducer().run();
    }

    public void run() throws InterruptedException, IOException {
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        KafkaProducer<String,String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown Hook:");
            logger.info("Shutting down Server Client");
            logger.info("Closing Producer");
            producer.close();
            logger.info("Shutdown completed");
        }));
        
        //Loop to send messages  to kafka
        while (fetchStreamData(msgQueue)) {
            String msg = null;
            
            while  (!msgQueue.isEmpty()){
            	 try {
                     msg = msgQueue.poll(1, TimeUnit.SECONDS);
                    
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
                 
                 if( msg != null){
                     logger.info("getted messages ===>"+msg);
                     producer.send(new ProducerRecord<>(Constants.KAFKA_TOPIC_NAME, msg), new Callback() {
                         @Override
                         public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                             if ( e != null){
                                 logger.error("Exception: ",e);
                             }
                         }
                     });
                     Thread.sleep(3000);
                 }
            }
           
        }
        
        logger.info("End of application");
    }



    public boolean fetchStreamData(BlockingQueue<String> msgQueue) throws InterruptedException, IOException {
    	@SuppressWarnings("resource")
		BufferedReader r = new BufferedReader(new FileReader(Constants.SOURCE_FILE_LOCATION));
        for (int i = 0; i < Constants.STREAM_API_BATCH_CNT * cnt ; i++){
        	if (i >= Constants.STREAM_API_BATCH_CNT * (cnt-1 )){
        		String line = r.readLine();
            	if (line == null ) return false;
            	msgQueue.put(line);
        	}else{
        		//logger.info(String.format("===============skipped first 0-%d",Constants.STREAM_API_BATCH_CNT*(cnt-1)));
        	}
        }
        cnt ++;
    	return true;
       
    }

    public KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BOOTSTRAP_SERVER_VALUE);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5" ); // Updated setting from Kafka 2.0

        // high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64*1024)); // 32 KB batch size ( default is 16 KB)

        // Create a Kafka Producer
        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);
        return  producer;
    }

}