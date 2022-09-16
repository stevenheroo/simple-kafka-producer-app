package com.github.skep.kafkaproducer.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ProducerDemo {


    public void publisher(KafkaProducer<String, String> producer, String msg) {
        Logger LOG = LoggerFactory.getLogger(ProducerDemo.class);

        //create a record
        ProducerRecord<String, String> record =
                new ProducerRecord<>("first-topic", null, msg);

        //send data
        //int finalI = i;
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                LOG.info("Error while producing" + e.getLocalizedMessage());
            } else {
        //LOG.info("KEY_" + finalI);
                LOG.info("\n-------------------" +
                        "\nMetadata Response\n" +
                        "------------------\n" +
                        "TOPIC: " + recordMetadata.topic() + "\n" +
                        "PARTITION: " + recordMetadata.partition() + "\n" +
                        "OFFSET: " + recordMetadata.offset() + "\n" +
                        "TIMESTAMP: " + recordMetadata.timestamp()
                );
                }
        });
        producer.close();
    }
}
