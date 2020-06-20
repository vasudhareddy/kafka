package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoAssignSeek {
    public static void main(String args[]) {
        System.out.println("hi");
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        long offsetToReadFrom = 15L;
        kafkaConsumer.assign(Arrays.asList(topicPartition));
        kafkaConsumer.seek(topicPartition, offsetToReadFrom);
         int numberOfMessagesTORead = 5;
        boolean keepOnReading = true;
         int numberOfMessagesReadSoFar = 0;
        //poll for new data

        while (keepOnReading) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar =numberOfMessagesReadSoFar+1;
                logger.info("key: " + record.key() + ", Value: " + record.value() + " ,Partition: " + record.partition() + ", offset:" + record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesTORead) {
                    keepOnReading = false;
                    break;
                }
            }
            logger.info("exiting the application");
        }
    }
}
