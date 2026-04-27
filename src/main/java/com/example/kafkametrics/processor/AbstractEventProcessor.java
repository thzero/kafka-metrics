package com.example.kafkametrics.processor;

import com.example.kafkametrics.kafka.KafkaProducerService;
import com.example.kafkametrics.model.EventHeader;
import com.example.kafkametrics.model.OutboundEnvelope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

/**
 * Template-method base for all event processors.
 *
 * <p>Owns the publish pipeline: calls {@link #processInternal} to produce a typed output,
 * serializes it to JSON via Jackson, then publishes to the configured output topic.
 * Subclasses only need to implement {@link #eventCode()} and {@link #processInternal}.
 *
 * @param <T> the output type returned by {@link #processInternal}; must be Jackson-serializable
 */
public abstract class AbstractEventProcessor<T> implements IEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(AbstractEventProcessor.class);

    protected final KafkaProducerService publisher;
    protected final ObjectMapper objectMapper;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${app.source-system-ent-cd}")
    private String sourceSystemEntCd;

    protected AbstractEventProcessor(KafkaProducerService publisher, ObjectMapper objectMapper) {
        this.publisher = publisher;
        this.objectMapper = objectMapper;
    }

    @Override
    public final void process(EventHeader incomingHeader, JsonNode payload) {
        String messageId = incomingHeader.messageId();

        T output = processInternal(incomingHeader, payload, messageId);

        JsonNode outputNode = objectMapper.valueToTree(output);
        EventHeader outboundHeader = incomingHeader.withSourceSystemEntCd(sourceSystemEntCd);
        OutboundEnvelope envelope = new OutboundEnvelope(outboundHeader, outputNode);
        JsonNode json = objectMapper.valueToTree(envelope);
        log.info("Publishing processed message messageId={} to topic={}", messageId, outputTopic);
        publisher.publish(messageId, json, outputTopic);
    }

    /**
     * Process the incoming message and return the output to be published.
     *
     * <p>The return value is serialized to JSON by the base class and published to the output
     * topic. Implementations need not interact with {@link KafkaProducerService} directly.
     *
     * @param incomingHeader the header from the deserialized incoming message; guaranteed non-null
     * @param payload        the payload JsonNode from the deserialized message
     * @param messageId      the messageId extracted from the message header
     * @return the output object to publish; must be Jackson-serializable
     * @throws com.example.kafkametrics.kafka.KafkaPublishException if the publish step fails
     * @throws com.example.kafkametrics.kafka.ProcessingException   if business logic fails
     */
    protected abstract T processInternal(EventHeader incomingHeader, JsonNode payload, String messageId);
}
