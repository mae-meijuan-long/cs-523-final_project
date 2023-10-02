## prepare jar
cd /home/cloudera/workspace/cs-523-final-project-gp/target

mvn clean && mvn package

## install kafka 

## start kafka server 
```
[cloudera@quickstart kafka_2.11-2.1.0]$ kafka-server-start.sh ./config/server.properties
```

## create kafka topic 
```
kafka-topics.sh --zookeeper localhost:2181 --create --topic cs-523-health-care --partitions 2 --replication-factor 1 --config max.message.bytes=64000 --config flush.messages=1
```

## test topic using sample producer and sample consumer
### test producer 
```
kafka-console-producer.sh --broker-list localhost:9092 --topic cs-523-health-care
```

[cloudera@quickstart /]$ kafka-console-producer.sh --broker-list localhost:9092 --topic cs-523-health-care
>


### test consumer 
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cs-523-health-care
```

[cloudera@quickstart Desktop]$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cs-523-health-care
[2023-10-01 14:33:16,564] WARN [Consumer clientId=consumer-1, groupId=console-consumer-67990] Error while fetching metadata with correlation id 2 : {cs-523-health-care=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)


## sample input data

```
Alabama,blank,blank,blank,blank
Alabama,Account Transfer Consumers whose Medicaid or CHIP Coverage was Terminated,Consumers Who Applied for Marketplace Coverage and were Determined Medicaid/CHIP-Eligible or Potentially Medicaid/CHIP-Eligible,13,3%
Alabama,Marketplace Consumers Not on Account Transfer whose Medicaid or CHIP Coverage was Terminated,Marketplace Consumers Not on Account Transfer whose Medicaid or CHIP Coverage was Terminated,363,NA
Alabama,Account Transfer Consumers whose Medicaid or CHIP Coverage was Terminated,Consumers who Applied for Marketplace Coverage and were Determined QHP-Eligible,79,21%
Alabama,Marketplace Consumers Not on Account Transfer whose Medicaid or CHIP Coverage was Terminated,Consumers Who Applied for Marketplace Coverage and were Determined Medicaid/CHIP-Eligible or Potentially Medicaid/CHIP-Eligible,40,NA
Alabama,Account Transfer Consumers whose Medicaid or CHIP Coverage was Terminated,Eligible for APTC,64,17%
```

## test producer

## shell script 
```
msgQueue
```


## create HBase table 
create 'healthcare', {NAME => 'basic', VERSIONS => 1}, {NAME => 'details', VERSIONS => 1}, SPLITS => ['4']

## start spark streaming job 



## demo steps 



