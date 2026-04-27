package com.example.kafkametrics.processor;

import com.example.kafkametrics.model.EventHeader;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Strategy interface for event processing.
 *
 * <p>Thrown exceptions are interpreted by {@link com.example.kafkametrics.kafka.KafkaConsumerListener}:
 * <ul>
 *   <li>{@link com.example.kafkametrics.kafka.KafkaPublishException} → {@code PUBLISH_ERROR} dead letter</li>
 *   <li>Any other exception → {@code PROCESSING_ERROR} dead letter</li>
 * </ul>
 */
public interface IEventProcessor {

    /**
     * Process the incoming message and publish the result to Kafka.
     *
     * @param incomingHeader the header from the deserialized incoming message
     * @param payload        the payload JsonNode from the deserialized message
     * @throws com.example.kafkametrics.kafka.KafkaPublishException if the publish step fails
     * @throws com.example.kafkametrics.kafka.ProcessingException   if business logic fails
     */
    void process(EventHeader incomingHeader, JsonNode payload);
}
