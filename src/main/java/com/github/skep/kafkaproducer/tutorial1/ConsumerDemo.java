package com.github.skep.kafkaproducer.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
    Logger LOG = LoggerFactory.getLogger(ProducerDemo.class);

    public ConsumerDemo(Properties prp) {
        subscribe(prp);
    }

    public void subscribe(Properties prp) {
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prp);
        consumer.subscribe(Collections.singletonList("first-topic"));
        //creating consumer thread
        LOG.info("Thread is creating");
        //latch for dealing multiiple threads
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread consumerThread =
                new ConsumerThread(consumer, latch);

        //start thread
        Thread thread = new Thread(consumerThread);
        thread.start();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            try {
                latch.await();
                LOG.info("Caught Shutdown Hook");
                consumerThread.shutDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOG.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Application is interrupted", e);
        }
        finally {
            LOG.info("Application is closing");
        }


    }

    public class ConsumerThread implements Runnable {
        private final CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        public ConsumerThread(
                KafkaConsumer<String, String> consumer,
                CountDownLatch latch) {
            this.consumer = consumer;
            this.latch = latch;
        }
        @Override
        public void run() {
            //subscribe consumer to topics
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
            try{
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("\n-------------------" +
                            "\nMetadata Response\n" +
                            "------------------\n" +
                            "TOPIC: " + record.topic() + "\n" +
                            "VALUE: " + record.value() + "\n" +
                            "PARTITION: " + record.partition() + "\n" +
                            "OFFSET: " + record.offset() + "\n" +
                            "TIMESTAMP: " + record.timestamp()
                    );
                }
            }
            catch (WakeupException e) {
                LOG.info("Shutdown signal is up");
            }
            finally {
                consumer.close();
                latch.countDown();
            }
        }
        public void shutDown() {
            //This method is thread-safe and is useful in particular to abort a long poll
            //used to break out of an active thread
            consumer.wakeup();
        }
    }


}
