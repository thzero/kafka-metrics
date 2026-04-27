package com.example.kafkametrics.services.processor.metrics.iif;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.CfmPgPoints;
import com.example.kafkametrics.repository.lookup.ICfmPgPointsRepository;
import com.example.kafkametrics.repository.lookup.Producer;
import com.example.kafkametrics.repository.metrics.iif.IIifMetricsPgPointsRepository;
import com.example.kafkametrics.services.metrics.lookup.IProducerService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class IifMetricsPgPointsProcessorServiceTest {

  @Mock private IIifMetricsPgPointsRepository pgPointsRepository;
  @Mock private IProducerService producerService;
  @Mock private ICfmPgPointsRepository cfmPgPointsRepository;

  private IifMetricsPgPointsProcessorService service;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    service =
        new IifMetricsPgPointsProcessorService(pgPointsRepository, producerService, cfmPgPointsRepository);
  }

  private ObjectNode validNode() {
    ObjectNode node = mapper.createObjectNode();
    node.put("agencyNbr", "AGENCY01");
    node.put("productFamilyEntCd", "transport");
    node.put("productSubFamilyEntCd", "auto");
    node.put("assetProductEntCd", "auto");
    return node;
  }

  private Producer producer(String cfmCd, String bonusPrimary) {
    Producer p = new Producer();
    p.setAgencyNbr("AGENCY01");
    p.setCfmCd(cfmCd);
    p.setBonusPrimaryAgencyNbr(bonusPrimary);
    return p;
  }

  private CfmPgPoints cfmPgPoints(int points) {
    CfmPgPoints c = new CfmPgPoints();
    c.setCfmCd("CFM001");
    c.setProductFamilyEntCd("transport");
    c.setProductSubFamilyEntCd("auto");
    c.setAssetProductEntCd("auto");
    c.setPgPointsValue(points);
    return c;
  }

  @Test
  void process_happyPath_enrichesNodeAndSavesToRepository() {
    when(producerService.findByAgencyNbr("AGENCY01")).thenReturn(producer("CFM001", "BONUS01"));
    when(cfmPgPointsRepository.findByCfmCdAndProductFamilyEntCdAndProductSubFamilyEntCdAndAssetProductEntCd(
            "CFM001", "transport", "auto", "auto"))
        .thenReturn(Optional.of(cfmPgPoints(150)));

    ObjectNode node = validNode();
    service.process("msg-1", "AGR001", node);

    assertThat(node.get("bonusPrimaryAgencyNbr").asText()).isEqualTo("BONUS01");
    assertThat(node.get("cfmCode").asText()).isEqualTo("CFM001");
    assertThat(node.get("pgPointsValue").asInt()).isEqualTo(150);
    assertThat(node.get("processedDt").asLong()).isGreaterThan(0);
    verify(pgPointsRepository).saveFromNode(eq("AGR001"), any());
  }

  @Test
  void process_missingAgencyNbr_throwsRequiredFieldException() {
    ObjectNode node = mapper.createObjectNode();
    node.put("productFamilyCd", "transport");
    node.put("productSubFamilyCd", "auto");
    node.put("assetProductCd", "auto");

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("agencyNbr");
  }

  @Test
  void process_missingProductFamilyEntCd_throwsRequiredFieldException() {
    ObjectNode node = mapper.createObjectNode();
    node.put("agencyNbr", "AGENCY01");
    node.put("productSubFamilyEntCd", "auto");
    node.put("assetProductEntCd", "auto");

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("productFamilyEntCd");
  }

  @Test
  void process_missingProductSubFamilyEntCd_throwsRequiredFieldException() {
    ObjectNode node = mapper.createObjectNode();
    node.put("agencyNbr", "AGENCY01");
    node.put("productFamilyEntCd", "transport");
    node.put("assetProductEntCd", "auto");

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("productSubFamilyEntCd");
  }

  @Test
  void process_missingAssetProductEntCd_throwsRequiredFieldException() {
    ObjectNode node = mapper.createObjectNode();
    node.put("agencyNbr", "AGENCY01");
    node.put("productFamilyEntCd", "transport");
    node.put("productSubFamilyEntCd", "auto");

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", node))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("assetProductEntCd");
  }

  @Test
  void process_cfmPgPointsNotFound_throwsRequiredFieldException() {
    when(producerService.findByAgencyNbr("AGENCY01")).thenReturn(producer("CFM001", "BONUS01"));
    when(cfmPgPointsRepository.findByCfmCdAndProductFamilyEntCdAndProductSubFamilyEntCdAndAssetProductEntCd(
            "CFM001", "transport", "auto", "auto"))
        .thenReturn(Optional.empty());

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", validNode()))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("CfmPgPoints not found");
  }

  @Test
  void process_producerNotFound_propagatesException() {
    when(producerService.findByAgencyNbr("AGENCY01"))
        .thenThrow(new RequiredFieldException("Producer not found for agencyNbr=AGENCY01"));

    assertThatThrownBy(() -> service.process("msg-1", "AGR001", validNode()))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("AGENCY01");
  }

  @Test
  void lookupCfmPgPoints_found_returnsCfmPgPoints() {
    CfmPgPoints expected = cfmPgPoints(200);
    when(cfmPgPointsRepository.findByCfmCdAndProductFamilyEntCdAndProductSubFamilyEntCdAndAssetProductEntCd(
            "CFM001", "transport", "auto", "auto"))
        .thenReturn(Optional.of(expected));

    CfmPgPoints result = service.lookupCfmPgPoints("CFM001", "transport", "auto", "auto");

    assertThat(result.getPgPointsValue()).isEqualTo(200);
  }

  @Test
  void lookupCfmPgPoints_notFound_throwsRequiredFieldException() {
    when(cfmPgPointsRepository.findByCfmCdAndProductFamilyEntCdAndProductSubFamilyEntCdAndAssetProductEntCd(
            "CFM001", "transport", "auto", "auto"))
        .thenReturn(Optional.empty());

    assertThatThrownBy(() -> service.lookupCfmPgPoints("CFM001", "transport", "auto", "auto"))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("CfmPgPoints not found");
  }
}
