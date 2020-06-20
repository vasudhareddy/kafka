package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0;i<10;i++) {

            //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hi kafka pipe"+Integer.toString(i));
            //send data using producer
            producer.send(producerRecord, (recordMetadata, e) -> {
                //execute every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    logger.info("topic name " + recordMetadata.topic().toString() + "\n" +
                            "partition " + recordMetadata.partition() + "\n"
                            + "timestamp " + recordMetadata.timestamp() + "\n"
                            + "offset " + recordMetadata.offset());
                } else {
                    logger.error("error occured", e);
                }
            });
            producer.flush();
        }
    }
}
