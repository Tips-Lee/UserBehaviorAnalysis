package com.tips.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception {
        writeToKafka("hot-items");
    }

    public static void writeToKafka(String topic) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "zk1:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String>  kafkaProducer = new KafkaProducer<>(properties);

        String inputPath ="./HotItemsAnalysis/src/main/resources/UserBehavior.csv";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputPath));
        String line;
        while ((line=bufferedReader.readLine()) != null){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            kafkaProducer.send(producerRecord);
        }

        kafkaProducer.close();

    }
}
