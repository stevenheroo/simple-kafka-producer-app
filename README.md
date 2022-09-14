# simple-kafka-producer-app
A simple kafka producer application

Process \
**_Step 1_** \
Download Kafka from https://kafka.apache.org/downloads \
and install I'm using a Linux OS, choose the one suitable for you OS. \
and follow installation instruction. \
After successful installation: \
1. Create a folder(called = data) in the Kafka directory,
which had two folders inside as well(zookeeper & kafka-logs).
2. Copy the path of the folder inside their parent Folder(data), and edit => [config/zookeeper.properties]
copy kafka-logs path and edit [server.properties]. \

**_Step 2_** \
_Start zookeeper_ \
1.zookeeper-server-start.sh config/zookeeper.properties \
_Start Kafka Server_ \
2.kafka-server-start.sh \

**`CLI commands`** \
#Create Topic \
3.kafka-topics.sh --create --topic **nameoftopic** --bootstrap-server localhost:9092 \
Use **kafka-topic.sh** to view documentation \
#Write events into topics (PRODUCER) \
4.kafka-console-producer.sh --topic **nameoftopic** --bootstrap-server localhost:9092 \
5.kafka-console-consumer.sh --topic **nameoftopic** --bootstrap-server localhost:9092 \

**When all is set**
Start writing producer java functions (Now and Future TODOs)
Create ProducerProperties, Create Producer, Create a record, Send Data ....
