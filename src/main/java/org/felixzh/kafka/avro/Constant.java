package org.felixzh.kafka.avro;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 博客地址：https://www.cnblogs.com/felixzh/
 * */
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
    static final String PRODUCER_BROKER = "felixzh:9092";
    static final String PRODUCER_TOPIC = "topic1";

    //consumer describe
    static final String CONSUMER_BROKER = "felixzh:9092";
    static final String CONSUMER_TOPIC = "topic1";
}
