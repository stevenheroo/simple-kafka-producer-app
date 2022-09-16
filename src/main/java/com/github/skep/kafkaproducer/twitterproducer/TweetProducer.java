package com.github.skep.kafkaproducer.twitterproducer;

import com.github.skep.kafkaproducer.tutorial1.ProducerDemo;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TweetProducer {

    public void publisher(Properties p) {
        Logger LOG = LoggerFactory.getLogger(TweetProducer.class);
        //message source,create twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = new TwitterClient().createClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.info("", e);
                client.stop();
            }
            if (msg != null) {
                new ProducerDemo().publisher(producer, msg);
                LOG.info(msg);
            }
        }
        LOG.info("END OF APPLICATION");
    }
}
