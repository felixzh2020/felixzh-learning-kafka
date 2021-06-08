package org.felixzh.kafka.sasl_ssl;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Date;
import java.util.Properties;


public class ProducerSSLDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "clusterdata1:50003");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //认证
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";");

        //ssl
        props.put("ssl.truststore.location", "D:\\client.truststore.jks");
        props.put("ssl.truststore.password", "test1234");
        props.put("ssl.endpoint.identification.algorithm", "");

        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, String>("test", new Date().toString()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                }

                if (metadata != null) {
                    System.out.println(metadata.offset());
                    System.out.println(metadata.toString());
                }
            }
        });

        producer.close();
    }
}
