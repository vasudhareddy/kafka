package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0;i<10;i++) {

            String topic ="first_topic";
            String value = "hellow world"+Integer.toString(i);
            String key ="Key_"+ Integer.toString(i);
            //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key, value);
            logger.info("key: "+ key); //log the key
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
            }).get();//block the send to make it synchronous -dont do this in prod
            producer.flush();
        }
    }
}
