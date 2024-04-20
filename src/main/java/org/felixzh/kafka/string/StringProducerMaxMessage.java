package org.felixzh.kafka.string;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;


/**
 * @author felixzh
 * 微信公众号：大数据从业者
 */
public class StringProducerMaxMessage {
    public static void main(String[] args) {
        Properties properties = new Properties();
        try {
            String conf = System.getenv("conf");
            properties.load(Files.newInputStream(Paths.get(conf)));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        int dataSizeKb = Integer.parseInt(properties.getProperty("dataSizeKb"));
        int dataCount = Integer.parseInt(properties.getProperty("dataCount"));
        String topic = properties.getProperty("topic");
        boolean sync = Boolean.getBoolean(properties.getProperty("sync"));

        int dataSizeInBytes = dataSizeKb * 1024;
        StringBuilder largeString = new StringBuilder();
        for (int i = 0; i < dataSizeInBytes; i++) {
            largeString.append("a");
        }

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, largeString.toString());

        for (int i = 1; i <= dataCount; i++) {
            try {
                if (sync)
                    producer.send(producerRecord).get();
                else
                    producer.send(producerRecord);
            } catch (Exception e) {
                e.printStackTrace();
                producer.close();
                System.exit(-1);
            }
        }
    }
}
