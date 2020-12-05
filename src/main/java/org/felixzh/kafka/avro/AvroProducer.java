package org.felixzh.kafka.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 博客地址：https://www.cnblogs.com/felixzh/
 * */
public class AvroProducer {

    private static Logger log = LoggerFactory.getLogger(AvroProducer.class);

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.PRODUCER_BROKER);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(Constant.USER_SCHEMA);
        Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);
        GenericData.Record record = new GenericData.Record(schema);
        long time = System.currentTimeMillis() / 1000;
        record.put("id", time);
        record.put("name", "name-" + time);
        byte[] recordBytes = injection.apply(record);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProps);
        producer.send(new ProducerRecord<>(Constant.PRODUCER_TOPIC, recordBytes));
        producer.close();

        log.info(Constant.USER_SCHEMA);
    }
}
