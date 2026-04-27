package com.example.kafkametrics.services.processor.metrics.iif;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkametrics.config.AppProperties;
import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.PolicyAor;
import com.example.kafkametrics.repository.lookup.PolicyMaster;
import com.example.kafkametrics.repository.metrics.iif.IIifMetricsRawRepository;
import com.example.kafkametrics.services.metrics.lookup.IPolicyAorService;
import com.example.kafkametrics.services.metrics.lookup.IPolicyMasterService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.concurrent.CompletionException;

@ExtendWith(MockitoExtension.class)
class IifMetricsRawProcessorServiceTest {

  @Mock private IPolicyMasterService policyMasterService;
  @Mock private IPolicyAorService policyAorService;
  @Mock private IIifMetricsRawRepository iifMetricsRawRepository;

  private IifMetricsRawProcessorService service;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    AppProperties props = new AppProperties();
    props.getProcessing().setLookupTimeoutMs(2000);
    service = new IifMetricsRawProcessorService(
        policyMasterService, policyAorService, iifMetricsRawRepository, props);
  }

  private PolicyMaster policyMaster(LocalDate effectiveDate, String scenarioCd) {
    PolicyMaster pm = new PolicyMaster();
    pm.setAgreementProductNumber("AGR001");
    pm.setOriginalPolicyEffectiveDate(effectiveDate);
    pm.setScenarioCd(scenarioCd);
    pm.setAssetProductEntCd("auto");
    return pm;
  }

  private PolicyAor policyAor(String agencyNbr, Boolean assigned) {
    PolicyAor aor = new PolicyAor();
    aor.setAgreementProductNumber("AGR001");
    aor.setAgencyNbr(agencyNbr);
    aor.setAssigned(assigned);
    return aor;
  }

  @Test
  void process_happyPath_enrichesNodeAndSavesToRepository() {
    when(policyMasterService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyMaster(LocalDate.of(2020, 6, 1), "SC01"));
    when(policyAorService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyAor("AGENCY01", true));

    ObjectNode node = mapper.createObjectNode();
    service.process("msg-1", "AGR001", node);

    assertThat(node.get("originalPolicyEffectiveDate").asText()).isEqualTo("2020-06-01");
    assertThat(node.get("scenarioCd").asText()).isEqualTo("SC01");
    assertThat(node.get("assetProductEntCd").asText()).isEqualTo("auto");
    assertThat(node.get("agencyNbr").asText()).isEqualTo("AGENCY01");
    assertThat(node.get("assigned").asBoolean()).isTrue();
    verify(iifMetricsRawRepository).saveFromNode(eq("msg-1"), eq("AGR001"), any());
  }

  @Test
  void process_nullOriginalPolicyEffectiveDate_throwsRequiredFieldException() {
    when(policyMasterService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyMaster(null, "SC01"));
    when(policyAorService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyAor("AGENCY01", true));

    ObjectNode node = mapper.createObjectNode();

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("originalPolicyEffectiveDate");
  }

  @Test
  void process_nullScenarioCd_throwsRequiredFieldException() {
    when(policyMasterService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyMaster(LocalDate.of(2020, 6, 1), null));
    when(policyAorService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyAor("AGENCY01", true));

    ObjectNode node = mapper.createObjectNode();

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("scenarioCd");
  }

  @Test
  void process_nullAgencyNbr_throwsRequiredFieldException() {
    when(policyMasterService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyMaster(LocalDate.of(2020, 6, 1), "SC01"));
    when(policyAorService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyAor(null, true));

    ObjectNode node = mapper.createObjectNode();

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("agencyNbr");
  }

  @Test
  void process_nullAssigned_throwsRequiredFieldException() {
    when(policyMasterService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyMaster(LocalDate.of(2020, 6, 1), "SC01"));
    when(policyAorService.findByAgreementProductNumber("AGR001"))
        .thenReturn(policyAor("AGENCY01", null));

    ObjectNode node = mapper.createObjectNode();

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("assigned");
  }

  @Test
  void process_policyMasterNotFound_propagatesException() {
    when(policyMasterService.findByAgreementProductNumber("MISSING"))
        .thenThrow(new RequiredFieldException("PolicyMaster not found for agreementProductNumber=MISSING"));

    ObjectNode node = mapper.createObjectNode();

    // supplyAsync wraps thrown exceptions in CompletionException on join()
    assertThatThrownBy(() -> service.process("msg-1", "MISSING", node))
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(RequiredFieldException.class);
  }
}
