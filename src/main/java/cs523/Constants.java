package cs523;

public class Constants {

	public static String KAFKA_TOPIC_NAME = "cs-523-health-care";
	public static String KAFKA_BOOTSTRAP_SERVER_VALUE = "localhost:9092";
	public static String SOURCE_FILE_LOCATION = "resouces/healthcare.gov-transitions-data.csv";
	public static String HBASE_ZK_QUORUM = "localhost:2181";
	public static Integer STREAM_API_BATCH_CNT = 50;
	public static String TARGET_HBASE_TABLE =  "healthcare";
	public static String INVALID_VALUE =  "blank";
	 
}
