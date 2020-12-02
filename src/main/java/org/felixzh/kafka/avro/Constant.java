package org.felixzh.kafka.avro;

class Constant {
    //schema describe
    static final String USER_SCHEMA = "{\n" +
            "    \"type\":\"record\",\n" +
            "    \"name\":\"Customer\",\n" +
            "    \"fields\":[\n" +
            "        {\"name\":\"id\",\"type\":\"int\"},\n" +
            "        {\"name\":\"name\",\"type\":\"string\"}\n" +
            "    ]\n" +
            "}";

    //producer describe
    static final String PRODUCER_BROKER = "172.29.1.149:9092";
    static final String PRODUCER_TOPIC = "topic1";

    //consumer describe
    static final String CONSUMER_BROKER = "172.29.1.149:9092";
    static final String CONSUMER_TOPIC = "topic1";
}
