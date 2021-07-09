package org.felixzh.kafka.AlterConfigPolicy;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AlterConfigPolicyImpl implements AlterConfigPolicy {

    Logger logger = LoggerFactory.getLogger(AlterConfigPolicy.class);

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
