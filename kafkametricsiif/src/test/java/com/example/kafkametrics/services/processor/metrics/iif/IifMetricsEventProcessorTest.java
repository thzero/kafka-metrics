package com.example.kafkametrics.services.processor.metrics.iif;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import com.example.kafkametrics.kafka.KafkaProducerService;
import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.model.EventHeader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class IifMetricsEventProcessorTest {

  @Mock private KafkaProducerService publisher;
  @Mock private IifMetricsRawProcessorService rawService;
  @Mock private IifMetricsIncludedProcessorService includedService;
  @Mock private IifMetricsPgPointsProcessorService pgPointsService;

  private IifMetricsEventProcessor processor;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    processor =
        new IifMetricsEventProcessor(
            publisher, mapper, rawService, includedService, pgPointsService);
    ReflectionTestUtils.setField(processor, "outputTopic", "test-output");
    ReflectionTestUtils.setField(processor, "sourceSystemEntCd", "TEST_SYS");
  }

  private EventHeader header(String messageId) {
    return new EventHeader(messageId, "interaction-1", "SOURCE");
  }

  @Test
  void process_happyPath_callsAllThreeServicesAndPublishes() {
    doNothing().when(publisher).publish(anyString(), any(JsonNode.class), anyString());

    ObjectNode payload = mapper.createObjectNode();
    payload.put("agreementProductNbr", "AGR001");

    processor.process(header("msg-1"), payload);

    verify(rawService).process(anyString(), anyString(), any(ObjectNode.class));
    verify(includedService).process(anyString(), anyString(), any(ObjectNode.class));
    verify(pgPointsService).process(anyString(), anyString(), any(ObjectNode.class));
    verify(publisher).publish(anyString(), any(JsonNode.class), anyString());
  }

  @Test
  void processInternal_setsPublishedDtOnNode() {
    doNothing().when(publisher).publish(anyString(), any(JsonNode.class), anyString());
    long before = System.currentTimeMillis();
    ObjectNode payload = mapper.createObjectNode();
    payload.put("agreementProductNbr", "AGR001");

    processor.process(header("msg-1"), payload);

    assertThat(payload.get("publishedDt").asLong()).isGreaterThanOrEqualTo(before);
    assertThat(payload.get("publishedDt").asLong()).isLessThanOrEqualTo(System.currentTimeMillis());
  }

  @Test
  void processInternal_missingAgreementProductNbr_throwsRequiredFieldException() {
    ObjectNode payload = mapper.createObjectNode();

    assertThatThrownBy(() -> processor.process(header("msg-1"), payload))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("agreementProductNbr");
  }

  @Test
  void processInternal_nonObjectPayload_throwsRequiredFieldException() {
    JsonNode arrayPayload = mapper.createArrayNode();

    assertThatThrownBy(() -> processor.process(header("msg-1"), arrayPayload))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("payload must be a JSON object");
  }
}
