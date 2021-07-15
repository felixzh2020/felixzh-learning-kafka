package org.felixzh.kafka.CreateTopicPolicy;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CreateTopicPolicyImpl implements CreateTopicPolicy {

    Logger logger = LoggerFactory.getLogger(CreateTopicPolicyImpl.class);

    @Override
    public void configure(Map<String, ?> configs) {
        logger.info("=========" + configs.toString());
    }

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        logger.info("=======" + requestMetadata.toString());
        /**
         * [root@felixzh bin]# ./kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe
         * Querying brokers for log directories information
         * Received log directory information from brokers 0
         * {"version":1,"brokers":[{"broker":0,"logDirs":[{"logDir":"/tmp/kafka-logs","error":null,"partitions":[{"partition":"t3-0","size":0,"offsetLag":0,"isFuture":false},{"partition":"t3-3","size":0,"offsetLag":0,"isFuture":false},{"partition":"t1-0","size":0,"offsetLag":0,"isFuture":false},{"partition":"t3-2","size":0,"offsetLag":0,"isFuture":false},{"partition":"t3-1","size":0,"offsetLag":0,"isFuture":false},{"partition":"t2-0","size":0,"offsetLag":0,"isFuture":false}]}]}]}
         * */
        //1.确定集群节点数
        //副本数，名称规范(只能用小写，不能包括.和下划线)
        // println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could " +
        //          "collide. To avoid issues it is best to use either, but not both.")
        //优点：插件化实现，插件单独维护，与Kafka源码解耦，方便维护升级
        //有可能由于误操作或者其他原因而创建了不符合运维规范的Topic，比如命名不规范，副本因子数太低等，这些都会影响后期的系统运维。
        //kafka-topics.sh --zookeeper已经废弃
        //2.分区数检查至少为节点数？？？？：3:900,
        //饶军：kafka PMC & Confluent联合创始人，指出计算集群topic和分区的公式。
    }

    @Override
    public void close() throws Exception {

    }


}
