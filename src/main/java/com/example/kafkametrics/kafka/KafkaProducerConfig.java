package com.example.kafkametrics.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.lang.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.transactional-id-prefix}")
    private String transactionalIdPrefix;

    @Value("${app.processing.processor-timeout-ms:10000}")
    private int processorTimeoutMs;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        if (processorTimeoutMs > 0) {
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, processorTimeoutMs);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, processorTimeoutMs / 2);
        }

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix(Objects.requireNonNull(transactionalIdPrefix));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(@NonNull ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, JsonNode> jsonProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        if (processorTimeoutMs > 0) {
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, processorTimeoutMs);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, processorTimeoutMs / 2);
        }

        DefaultKafkaProducerFactory<String, JsonNode> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix(Objects.requireNonNull(transactionalIdPrefix) + "-json");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, JsonNode> jsonKafkaTemplate(@NonNull ProducerFactory<String, JsonNode> jsonProducerFactory) {
        return new KafkaTemplate<>(jsonProducerFactory);
    }
}
