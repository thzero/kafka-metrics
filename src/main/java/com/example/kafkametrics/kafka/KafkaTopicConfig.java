package com.example.kafkametrics.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Objects;

/**
 * Declares Kafka topics as Spring beans.
 *
 * <p>Spring Boot's {@code KafkaAdmin} picks these up at startup and issues
 * {@code CreateTopics} requests to the broker. If a topic already exists with
 * compatible settings the call is a no-op. This eliminates the transient
 * {@code UNKNOWN_TOPIC_OR_PARTITION} warnings that appear when a transactional
 * producer first references a topic that hasn't been created yet.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name(Objects.requireNonNull(inputTopic)).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder.name(Objects.requireNonNull(outputTopic)).partitions(1).replicas(1).build();
    }
}
