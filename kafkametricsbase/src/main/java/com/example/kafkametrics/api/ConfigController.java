package com.example.kafkametrics.api;

import com.example.kafkametrics.config.AppProperties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Returns the current running configuration as JSON. Useful for verifying that
 * the deployed instance has the expected topic names, delays, and active evaluators.
 */
@RestController
@RequestMapping("/api/config")
public class ConfigController {

    private final AppProperties appProperties;
    private final String bootstrapServers;
    private final String consumerGroupId;
    private final int consumerConcurrency;
    private final String inputTopic;
    private final String outputTopic;

    public ConfigController(
            AppProperties appProperties,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.consumer.group-id}") String consumerGroupId,
            @Value("${kafka.consumer.concurrency}") int consumerConcurrency,
            @Value("${kafka.topic.input}") String inputTopic,
            @Value("${kafka.topic.output}") String outputTopic) {
        this.appProperties = appProperties;
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.consumerConcurrency = consumerConcurrency;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @GetMapping
    public ConfigView getConfig() {
        AppProperties.Processing p = appProperties.getProcessing();
        return new ConfigView(
                new KafkaView(bootstrapServers, consumerGroupId, consumerConcurrency, inputTopic, outputTopic),
                new AppView(p.getProcessorTimeoutMs(), p.getWorkerThreads()));
    }

    // -------------------------------------------------------------------------
    // Nested types
    // -------------------------------------------------------------------------

    record ConfigView(KafkaView kafka, AppView app) {}

    record KafkaView(
            String bootstrapServers,
            String consumerGroupId,
            int consumerConcurrency,
            String inputTopic,
            String outputTopic) {}

    record AppView(
            long processorTimeoutMs,
            int workerThreads) {}
}
