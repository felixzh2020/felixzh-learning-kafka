package org.felixzh.kafka.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 博客地址：https://www.cnblogs.com/felixzh/
 * */
public class AvroConsumer {
    static Logger log = LoggerFactory.getLogger(AvroConsumer.class);

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.CONSUMER_BROKER);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "AvroKafkaConsumer");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(Constant.CONSUMER_TOPIC));

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(Constant.USER_SCHEMA);
        Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);

        while (true) {
            try {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(50));
                for (ConsumerRecord<String, byte[]> record : records) {
                    GenericRecord genericRecord = injection.invert(record.value()).get();
                    log.info(record.key() + ":" + genericRecord.get("id") + "\t" + genericRecord.get("name") + "\t");
                    System.out.println();
                }
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
            }
        }

    }
}
