### Soonam's flink practice project

### Running flink with kafka

### Download kafka :
kafka_2.12-2.2.0.tgz

### Untar :
tar xf kafka_2.12-2.2.0.tgz

### Zookeeper server
./bin/zookeeper-server-start.sh ./config/zookeeper.properties

### Start Kafka Server
./bin/kafka-server-start.sh ./config/server.properties


### Create topic
./bin/kafka-topics.sh --create --topic test --zookeeper localhost:2181 --partitions 1 --replication-factor 1

### Producer
./bin/kafka-console-consumer.sh --topic test --zookeeper localhost:2181

### consumer
./bin/kafka-console-consumer.sh --topic test --bootstrap-server localhost:2181 --from-beginning


### Connect From Java
```
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.12</artifactId>
            <version>${flink.version}</version>
        </dependency>

```

Run KafkaConsumerTest passing arguments as :
--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup

### Current Project Info
This project contains few examples of Flink basic programs.It also contains Misra-Gries implementation 
using flink operator-state.

