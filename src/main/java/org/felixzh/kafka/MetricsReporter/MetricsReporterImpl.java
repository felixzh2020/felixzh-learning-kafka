package org.felixzh.kafka.MetricsReporter;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.List;
import java.util.Map;

public class MetricsReporterImpl implements MetricsReporter {
    @Override
    public void init(List<KafkaMetric> list) {
        
    }

    @Override
    public void metricChange(KafkaMetric kafkaMetric) {

    }

    @Override
    public void metricRemoval(KafkaMetric kafkaMetric) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
