package com.example.kafkametrics.services.processor.metrics.iif;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkametrics.repository.metrics.iif.IIifMetricInclusionRepository;
import com.example.kafkametrics.services.metrics.lookup.IExclusionRuleService;

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
  @Mock private IExclusionRuleService exclusionRuleService;

  private IifMetricsIncludedProcessorService service;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    service = new IifMetricsIncludedProcessorService(inclusionRepository, exclusionRuleService);
  }

  @Test
  void process_setsExcludedIndFalseOnNode() {
    ObjectNode node = mapper.createObjectNode();

    service.process("msg-1", "AGR001", node);

    assertThat(node.get("excludedInd").asBoolean()).isFalse();
  }

  @Test
  void process_savesToRepositoryWithCorrectArguments() {
    ObjectNode node = mapper.createObjectNode();

    service.process("msg-1", "AGR001", node);

    verify(inclusionRepository).saveFromNode(eq("AGR001"), eq(false), any());
  }

  @Test
  void process_whenRuleExcludes_setsExcludedIndTrueOnNode() {
    ObjectNode node = mapper.createObjectNode();
    when(exclusionRuleService.isExcluded(node)).thenReturn(true);

    service.process("msg-1", "AGR001", node);

    assertThat(node.get("excludedInd").asBoolean()).isTrue();
    verify(inclusionRepository).saveFromNode(eq("AGR001"), eq(true), any());
  }
}
