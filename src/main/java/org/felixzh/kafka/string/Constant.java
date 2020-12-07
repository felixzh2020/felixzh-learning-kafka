package org.felixzh.kafka.string;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 博客地址：https://www.cnblogs.com/felixzh/
 */
class Constant {

    //producer describe
    static final String PRODUCER_BROKER = "felixzh:9092";
    static final String PRODUCER_CANAL_TOPIC = "canal_products_binlog";
    static final String PRODUCER_DEBAZIUM_TOPIC = "debezium_products_binlog";

    //consumer describe
    static final String CONSUMER_BROKER = "felixzh:9092";
    static final String CONSUMER_TOPIC = "debezium_products_binlog";
}
