package org.felixzh.kafka.string;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class StringProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "felixzh:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);


        producer.send(new ProducerRecord<String, String>("products_binlog", msg.replace("\r\n", "")));

        producer.close();
    }

    private static final String msg = "{\n" +
            "  \"data\": [\n" +
            "    {\n" +
            "      \"id\": \"111\",\n" +
            "      \"name\": \"scooter\",\n" +
            "      \"description\": \"Big 2-wheel scooter\",\n" +
            "      \"weight\": \"5.18\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"database\": \"inventory\",\n" +
            "  \"es\": 1589373560000,\n" +
            "  \"id\": 9,\n" +
            "  \"isDdl\": false,\n" +
            "  \"mysqlType\": {\n" +
            "    \"id\": \"INTEGER\",\n" +
            "    \"name\": \"VARCHAR(255)\",\n" +
            "    \"description\": \"VARCHAR(512)\",\n" +
            "    \"weight\": \"FLOAT\"\n" +
            "  },\n" +
            "  \"old\": [\n" +
            "    {\n" +
            "      \"weight\": \"5.15\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"pkNames\": [\n" +
            "    \"id\"\n" +
            "  ],\n" +
            "  \"sql\": \"\",\n" +
            "  \"sqlType\": {\n" +
            "    \"id\": 4,\n" +
            "    \"name\": 12,\n" +
            "    \"description\": 12,\n" +
            "    \"weight\": 7\n" +
            "  },\n" +
            "  \"table\": \"products\",\n" +
            "  \"ts\": 1589373560798,\n" +
            "  \"type\": \"UPDATE\"\n" +
            "}";
}
