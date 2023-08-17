# Run Kafka sever
### Go to the directory of kafka
```#cd C:\Kafka\kafka_2.13-3.5.0```
### Start zoo keeper
```.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties```
### Start kafka server
```.\bin\windows\kafka-server-start.bat .\config\server.properties```
### Create kafka topic - kafka topic name: "log_tracking_k"
```.\bin\windows\kafka-topics.bat --create --bootstrap-server 192.168.56.1:9092 --replication-factor 1 --partitions 1 --topic logtrackingk```

# Run python
### Go to the directory of project
```#cd data_recruitment_pipelines```
### Send log data to kafka topic through kafka producer
``` python kafka/kafka_producer_faking_logdata.py"```

<img width="352" alt="image" src="images/kafka_producer_faking_logdata.png">


### Read data from kafka topic and storage in Data Lake: Cassandra
``` python kafka/consume_from_kafka_to_cassandra.py"```

<img width="352" alt="image" src="images/consumer_from_kafka_to_cassandradb.png">
