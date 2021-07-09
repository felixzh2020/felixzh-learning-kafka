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
    }

    @Override
    public void close() throws Exception {

    }


}
