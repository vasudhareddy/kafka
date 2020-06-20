package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerDemoWithThreads {
    public static void main(String args[]) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {

    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(1);
         Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
         logger.info("creating consumer in thread");
        Runnable myConsumerRunnable= new ConsumerRunnable(latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("shutdown");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();


        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public class ConsumerRunnable implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        //create consumer
        KafkaConsumer<String, String> kafkaConsumer;
        private CountDownLatch latch;

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "MY-FIFTH-APPLICATION");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Collections.singleton("first_topic"));

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    records.forEach(record -> {
                        logger.info("key: " + record.key() + ", Value: " + record.value() + " ,Partition: " + record.partition() + ", offset:" + record.offset());
                    });
                }
            } catch (WakeupException e) {
                logger.info("received shutdown signal");

            } finally {

                kafkaConsumer.close();
                ;
                latch.countDown();
            }
        }


        public void shutdown() {
            //the wakeup() method is aspecial method to interrupt conusmer poll
            //it will throw an exception
            kafkaConsumer.wakeup();

        }
    }
}
