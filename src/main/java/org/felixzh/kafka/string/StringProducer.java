package org.felixzh.kafka.string;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 博客地址：https://www.cnblogs.com/felixzh/
 */
public class StringProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.PRODUCER_BROKER);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);


        producer.send(new ProducerRecord<String, String>(Constant.PRODUCER_DEBAZIUM_TOPIC, debezium_msg.replace("\r\n", "")));

        producer.close();
    }

    private static final String canal_msg = "{\n" +
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

    private static final String debezium_msg = "{\n" +
            "  \"before\": {\n" +
            "    \"id\": 111,\n" +
            "    \"name\": \"scooter\",\n" +
            "    \"description\": \"Big 2-wheel scooter\",\n" +
            "    \"weight\": 5.18\n" +
            "  },\n" +
            "  \"after\": {\n" +
            "    \"id\": 111,\n" +
            "    \"name\": \"scooter\",\n" +
            "    \"description\": \"Big 2-wheel scooter\",\n" +
            "    \"weight\": 5.15\n" +
            "  },\n" +
            "\"source\": { \n" +
            "      \"version\": \"1.1.2.Final\",\n" +
            "      \"name\": \"mysql-server-1\",\n" +
            "      \"connector\": \"mysql\",\n" +
            "      \"name\": \"mysql-server-1\",\n" +
            "      \"ts_ms\": 1465581,\n" +
            "      \"snapshot\": false,\n" +
            "      \"db\": \"inventory\",\n" +
            "      \"table\": \"customers\",\n" +
            "      \"server_id\": 223344,\n" +
            "      \"gtid\": null,\n" +
            "      \"file\": \"mysql-bin.000003\",\n" +
            "      \"pos\": 484,\n" +
            "      \"row\": 0,\n" +
            "      \"thread\": 7,\n" +
            "      \"query\": \"UPDATE customers SET first_name='Anne Marie' WHERE id=1004\"\n" +
            "    }," +
            "  \"op\": \"u\",\n" +
            "  \"ts_ms\": 1589362330904,\n" +
            "  \"transaction\": null\n" +
            "}";
}
