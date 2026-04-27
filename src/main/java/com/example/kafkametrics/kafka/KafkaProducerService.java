package com.example.kafkametrics.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Unified Kafka publisher.
 * Used by both the siphon fast-path in {@link KafkaConsumerListener} and by
 * {@link com.example.kafkametrics.processor.IEventProcessor} implementations.
 *
 * <p>Two overloads are provided:
 * <ul>
 *   <li>{@link #publish(String, String, String)} — for pre-serialized strings (e.g. siphon pass-through)</li>
 *   <li>{@link #publish(String, JsonNode, String)} — for dynamic JSON objects; serialized by Kafka's JsonSerializer</li>
 * </ul>
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, JsonNode> jsonKafkaTemplate;

    public KafkaProducerService(
            @Qualifier("kafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
            @Qualifier("jsonKafkaTemplate") KafkaTemplate<String, JsonNode> jsonKafkaTemplate) {
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate must not be null");
        this.jsonKafkaTemplate = Objects.requireNonNull(jsonKafkaTemplate, "jsonKafkaTemplate must not be null");
    }

    /**
     * Publishes a pre-serialized JSON string to the specified Kafka topic.
     * Use this overload for pass-through payloads (e.g. the siphon fast-path).
     *
     * @param key     Kafka record key (typically the messageId) — used for partition routing
     * @param payload raw JSON string to publish as the record value
     * @param topic   target Kafka topic
     * @throws KafkaPublishException if the send fails
     */
    public void publish(@Nullable String key, String payload, String topic) {
        kafkaTemplate.executeInTransaction(ops -> {
            try {
                ops.send(new ProducerRecord<>(
                    Objects.requireNonNull(topic, "topic must not be null"),
                    key, // null is valid for Kafka keys (default partition assignment)
                    Objects.requireNonNull(payload, "payload must not be null")
                )).get();
            } catch (ExecutionException e) {
                throw new KafkaPublishException(
                        "Failed to publish to topic=" + topic + " key=" + key, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KafkaPublishException("Interrupted publishing to topic=" + topic, e);
            }
            return Objects.requireNonNull(Boolean.TRUE);
        });
        log.info("Published to topic={} key={}", topic, key);
    }

    /**
     * Publishes a dynamic JSON object to the specified Kafka topic.
     * Uses a dedicated {@link KafkaTemplate} backed by Jackson's {@code JsonSerializer}.
     * No manual serialization needed — pass the {@link JsonNode} directly.
     *
     * @param key     Kafka record key (typically the messageId) — used for partition routing
     * @param payload JSON node to publish as the record value
     * @param topic   target Kafka topic
     * @throws KafkaPublishException if the send fails
     */
    public void publish(@Nullable String key, JsonNode payload, String topic) {
        jsonKafkaTemplate.executeInTransaction(ops -> {
            try {
                ops.send(new ProducerRecord<>(
                    Objects.requireNonNull(topic, "topic must not be null"),
                    key, // null is valid for Kafka keys (default partition assignment)
                    Objects.requireNonNull(payload, "payload must not be null")
                )).get();
            } catch (ExecutionException e) {
                throw new KafkaPublishException(
                        "Failed to publish to topic=" + topic + " key=" + key, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KafkaPublishException("Interrupted publishing to topic=" + topic, e);
            }
            return Objects.requireNonNull(Boolean.TRUE);
        });
        log.info("Published to topic={} key={}", topic, key);
    }
}
