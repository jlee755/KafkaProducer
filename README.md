# KafkaProducer

This is a small Java project for generating test messages to be sent to a Kafka topic.

## Usage
```
usage: kafka-producer
 -c,--threads <arg>    Thread Count
 -h,--host <arg>       Kafka Host & IP list
 -m,--messages <arg>   Messages Count
 -t,--topic <arg>      Topic Name
```

## Example
To run 4 threads producing 5 messages each and send them to a topic named 'marklogic' on a server named 'myKafka'

`java -jar ./kafka-producer-1.0-SNAPSHOT.jar -c 4 -m 5 -h myKafka:9092 -t marklogic`
