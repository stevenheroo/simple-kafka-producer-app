package com.github.skep.kafkaproducer.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public ProducerDemo(Properties prp) {
        publisher(prp);
    }
    public void publisher(Properties p) {
        Logger LOG = LoggerFactory.getLogger(ProducerDemo.class);

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);

        for (int i = 0; i < 8; i++) {
            //create a record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first-topic", "" + i, "hello world3");

            //send data
            int finalI = i;
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    LOG.info("Error while producing" + e.getLocalizedMessage());
                } else {
                    LOG.info("KEY_" + finalI);
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
        }
        producer.close();
    }
}
