package com.example.kafkametrics.services.processor.metrics.iif;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.example.kafkametrics.repository.metrics.iif.IIifMetricInclusionRepository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IifMetricsIncludedProcessorServiceTest {

  @Mock private IIifMetricInclusionRepository inclusionRepository;

  private IifMetricsIncludedProcessorService service;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    service = new IifMetricsIncludedProcessorService(inclusionRepository);
  }

  @Test
  void process_setsExcludedIndFalseOnNode() {
    ObjectNode node = mapper.createObjectNode();

    service.process("msg-1", "AGR001", node);

    assertThat(node.get("excludedInd").asBoolean()).isFalse();
  }

  @Test
  void process_setsProcessedDtOnNode() {
    ObjectNode node = mapper.createObjectNode();
    long before = System.currentTimeMillis();

    service.process("msg-1", "AGR001", node);

    long processedDt = node.get("processedDt").asLong();
    assertThat(processedDt).isGreaterThanOrEqualTo(before);
    assertThat(processedDt).isLessThanOrEqualTo(System.currentTimeMillis());
  }

  @Test
  void process_savesToRepositoryWithCorrectArguments() {
    ObjectNode node = mapper.createObjectNode();

    service.process("msg-1", "AGR001", node);

    verify(inclusionRepository).saveFromNode(eq("AGR001"), eq(false), any());
  }
}
