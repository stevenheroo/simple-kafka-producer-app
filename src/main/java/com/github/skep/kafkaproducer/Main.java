package com.github.skep.kafkaproducer;

import com.github.skep.kafkaproducer.tutorial1.ConsumerDemo;
import com.github.skep.kafkaproducer.tutorial1.MyStaticValues;
import com.github.skep.kafkaproducer.tutorial1.ProducerDemo;
import com.github.skep.kafkaproducer.twitterproducer.TweetProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        System.out.println("Enter Option (NB: 1 to publish): ");
        int a = input.nextInt();
        Properties p;
        Logger LOG = LoggerFactory.getLogger(Main.class);

        if (a == 1) {
            p = producerConfig();
            new TweetProducer().publisher(p);
        }
        else if (a == 2){
            p = consumerConfig();
            new ConsumerDemo(p);
        }
    }
    public static Properties producerConfig() {
        //create properties
        Properties publishProps = new Properties();
        publishProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, new MyStaticValues().bootstrap());
        publishProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        publishProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return publishProps;
    }
    public static Properties consumerConfig() {
        Properties subscriberProps = new Properties();
        subscriberProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, new MyStaticValues().bootstrap());
        subscriberProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        subscriberProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        subscriberProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-first-group");
        subscriberProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return subscriberProps;
    }
}
