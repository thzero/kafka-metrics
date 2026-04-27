package com.example.kafkametrics.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.kafkametrics.KafkaMetricsApplication;
import com.example.kafkametrics.control.IPublishedRecordRepository;
import com.example.kafkametrics.control.IReceivedRecordRepository;
import com.example.kafkametrics.model.EventHeader;
import com.example.kafkametrics.model.KafkaMessage;
import com.example.kafkametrics.model.OutboundEnvelope;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = KafkaMetricsApplication.class)
@ActiveProfiles("integration")
@EmbeddedKafka(partitions = 1, topics = {"test-input-topic", "test-output-topic"})
@TestPropertySource(properties = {
        "kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class KafkaIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private IReceivedRecordRepository receivedRecordRepository;

    @Autowired
    private IPublishedRecordRepository publishedRecordRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Test
    void messageFlowProducesOutputAndWritesControlRecords() throws Exception {
        // Arrange: set up a consumer on the output topic to capture published messages
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-consumer-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        BlockingQueue<ConsumerRecord<String, String>> outputRecords = new LinkedBlockingQueue<>();
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProps = new ContainerProperties(outputTopic);
        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProps);
        container.setupMessageListener((MessageListener<String, String>) outputRecords::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

        // Act: publish a message to the input topic
        KafkaMessage inputMessage = new KafkaMessage(
                new EventHeader("a0000000-0000-0000-0000-000000000001", "iid-integration", "TEST", null, null),
                objectMapper.createObjectNode());
        String payload = Objects.requireNonNull(objectMapper.writeValueAsString(inputMessage));

        Map<String, Object> producerProps = Objects.requireNonNull(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        org.springframework.kafka.core.DefaultKafkaProducerFactory<String, String> pf =
                new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(producerProps);
        org.springframework.kafka.core.KafkaTemplate<String, String> template =
                new org.springframework.kafka.core.KafkaTemplate<>(pf);
        template.send(Objects.requireNonNull(inputTopic), payload);

        // Assert: message appears on output topic
        ConsumerRecord<String, String> received = outputRecords.poll(10, TimeUnit.SECONDS);
        assertThat(received).isNotNull();

        OutboundEnvelope outputEnvelope = objectMapper.readValue(received.value(), OutboundEnvelope.class);
        assertThat(outputEnvelope.header().messageId()).isEqualTo("a0000000-0000-0000-0000-000000000001");

        // Assert: RECEIVED and PUBLISHED control records exist
        assertThat(receivedRecordRepository.existsByMessageId("a0000000-0000-0000-0000-000000000001")).isTrue();
        assertThat(publishedRecordRepository.existsByMessageId("a0000000-0000-0000-0000-000000000001")).isTrue();

        container.stop();
        pf.destroy();
    }
}
