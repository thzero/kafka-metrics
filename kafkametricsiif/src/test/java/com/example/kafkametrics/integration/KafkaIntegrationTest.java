package com.example.kafkametrics.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.example.kafkametrics.KafkaMetricsApplication;
import com.example.kafkametrics.control.IPublishedRecordRepository;
import com.example.kafkametrics.control.IReceivedRecordRepository;
import com.example.kafkametrics.deadletter.DeadLetterRecord;
import com.example.kafkametrics.deadletter.IDeadLetterRepository;
import com.example.kafkametrics.model.EventHeader;
import com.example.kafkametrics.model.KafkaMessage;
import com.example.kafkametrics.repository.lookup.CfmPgPoints;
import com.example.kafkametrics.repository.lookup.ICfmPgPointsRepository;
import com.example.kafkametrics.repository.lookup.IPolicyAorRepository;
import com.example.kafkametrics.repository.lookup.IPolicyMasterRepository;
import com.example.kafkametrics.repository.lookup.IProducerRepository;
import com.example.kafkametrics.repository.lookup.PolicyAor;
import com.example.kafkametrics.repository.lookup.PolicyMaster;
import com.example.kafkametrics.repository.lookup.Producer;
import com.example.kafkametrics.services.metrics.lookup.ICfmPgPointsService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
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

import java.time.LocalDate;
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

    private static final String AGREEMENT_PRODUCT_NBR  = "TEST0000000001A";
    private static final String AGENCY_NBR             = "0TEST1";
    private static final String CFM_CD                 = "CFM0001";
    private static final String PRODUCT_FAMILY_ENT_CD     = "transport";
    private static final String PRODUCT_SUB_FAMILY_ENT_CD = "auto";
    private static final String ASSET_PRODUCT_ENT_CD      = "auto";

    @Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired private IReceivedRecordRepository receivedRecordRepository;
    @Autowired private IPublishedRecordRepository publishedRecordRepository;
    @Autowired private IPolicyMasterRepository policyMasterRepository;
    @Autowired private IPolicyAorRepository policyAorRepository;
    @Autowired private IProducerRepository producerRepository;
    @Autowired private ICfmPgPointsRepository cfmPgPointsRepository;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private IDeadLetterRepository deadLetterRepository;
    @Autowired private ICfmPgPointsService cfmPgPointsService;

    @Value("${kafka.topic.input}")  private String inputTopic;
    @Value("${kafka.topic.output}") private String outputTopic;

    @BeforeEach
    void seedReferenceData() {
        PolicyMaster pm = new PolicyMaster();
        pm.setAgreementProductNumber(AGREEMENT_PRODUCT_NBR);
        pm.setOriginalPolicyEffectiveDate(LocalDate.of(2020, 1, 1));
        pm.setScenarioCd("NEW");
        pm.setAssetProductEntCd(ASSET_PRODUCT_ENT_CD);
        policyMasterRepository.save(pm);

        PolicyAor aor = new PolicyAor();
        aor.setAgreementProductNumber(AGREEMENT_PRODUCT_NBR);
        aor.setAgencyNbr(AGENCY_NBR);
        aor.setAssigned(true);
        policyAorRepository.save(aor);

        Producer producer = new Producer();
        producer.setAgencyNbr(AGENCY_NBR);
        producer.setBonusPrimaryAgencyNbr(AGENCY_NBR);
        producer.setCfmCd(CFM_CD);
        producerRepository.save(producer);

        CfmPgPoints cfm = new CfmPgPoints();
        cfm.setCfmCd(CFM_CD);
        cfm.setProductFamilyEntCd(PRODUCT_FAMILY_ENT_CD);
        cfm.setProductSubFamilyEntCd(PRODUCT_SUB_FAMILY_ENT_CD);
        cfm.setAssetProductEntCd(ASSET_PRODUCT_ENT_CD);
        cfm.setPgPointsValue(25);
        cfmPgPointsRepository.save(cfm);

        // Refresh cache so it reflects the data inserted above
        cfmPgPointsService.refresh();
    }

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

        // Act: publish a message with the fields required by IifMetricsEventProcessor
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("agreementProductNbr", AGREEMENT_PRODUCT_NBR);
        payload.put("productFamilyEntCd", PRODUCT_FAMILY_ENT_CD);
        payload.put("productSubFamilyEntCd", PRODUCT_SUB_FAMILY_ENT_CD);

        String messageId = "a0000000-0000-0000-0000-000000000001";
        KafkaMessage inputMessage = new KafkaMessage(
                new EventHeader(messageId, "iid-integration", null),
                payload);
        String serialized = Objects.requireNonNull(objectMapper.writeValueAsString(inputMessage));

        Map<String, Object> producerProps = Objects.requireNonNull(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        org.springframework.kafka.core.DefaultKafkaProducerFactory<String, String> pf =
                new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(producerProps);
        org.springframework.kafka.core.KafkaTemplate<String, String> template =
                new org.springframework.kafka.core.KafkaTemplate<>(pf);
        template.send(Objects.requireNonNull(inputTopic), serialized);

        // Assert: message appears on output topic
        ConsumerRecord<String, String> received = outputRecords.poll(10, TimeUnit.SECONDS);
        if (received == null) {
            java.util.List<DeadLetterRecord> dlq = deadLetterRepository.findAll();
            String dlqSummary = dlq.stream()
                    .map(r -> r.getReasonCode() + ": " + r.getRawPayload())
                    .collect(java.util.stream.Collectors.joining(", "));
            fail("No output received after 10s. Dead letters: [" + dlqSummary + "]");
        }

        KafkaMessage outputEnvelope = objectMapper.readValue(received.value(), KafkaMessage.class);
        assertThat(outputEnvelope.header().messageId()).isEqualTo(messageId);

        // Assert: RECEIVED and PUBLISHED control records exist
        assertThat(receivedRecordRepository.existsByMessageId(messageId)).isTrue();
        assertThat(publishedRecordRepository.existsByMessageId(messageId)).isTrue();

        container.stop();
        pf.destroy();
    }
}
