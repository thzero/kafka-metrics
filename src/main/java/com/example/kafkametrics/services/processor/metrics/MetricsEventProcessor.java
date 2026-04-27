package com.example.kafkametrics.services.processor.metrics;

import com.example.kafkametrics.kafka.KafkaProducerService;
import com.example.kafkametrics.services.processor.AbstractEventProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class MetricsEventProcessor<T> extends AbstractEventProcessor<T> {

    protected MetricsEventProcessor(KafkaProducerService publisher, ObjectMapper objectMapper) {
        super(publisher, objectMapper);
    }
}
