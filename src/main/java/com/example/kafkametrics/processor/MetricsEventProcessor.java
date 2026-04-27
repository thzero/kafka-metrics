package com.example.kafkametrics.processor;

import com.example.kafkametrics.kafka.KafkaProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class MetricsEventProcessor<T> extends AbstractEventProcessor<T> {

    protected MetricsEventProcessor(KafkaProducerService publisher, ObjectMapper objectMapper) {
        super(publisher, objectMapper);
    }
}
