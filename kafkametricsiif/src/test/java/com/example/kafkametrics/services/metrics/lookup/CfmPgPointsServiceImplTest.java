package com.example.kafkametrics.services.metrics.lookup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.CfmPgPoints;
import com.example.kafkametrics.repository.lookup.ICfmPgPointsRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

@ExtendWith(MockitoExtension.class)
class CfmPgPointsServiceImplTest {

  @Mock private ICfmPgPointsRepository repository;

  private CfmPgPointsServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new CfmPgPointsServiceImpl(repository);
  }

  private CfmPgPoints entry(String cfmCode, String productFamilyEntCd, String productSubFamilyEntCd, String assetEntCd, int points) {
    CfmPgPoints c = new CfmPgPoints();
    c.setCfmCd(cfmCode);
    c.setProductFamilyEntCd(productFamilyEntCd);
    c.setProductSubFamilyEntCd(productSubFamilyEntCd);
    c.setAssetProductEntCd(assetEntCd);
    c.setPgPointsValue(points);
    return c;
  }

  @Test
  void refresh_loadsAllEntries() {
    when(repository.findAll()).thenReturn(List.of(
        entry("CFM001", "FAM1", "SUB1", "PROD1", 100),
        entry("CFM001", "FAM1", "SUB1", "PROD2", 200)
    ));

    service.refresh();

    assertThat(service.lookup("CFM001", "FAM1", "SUB1", "PROD1").getPgPointsValue()).isEqualTo(100);
    assertThat(service.lookup("CFM001", "FAM1", "SUB1", "PROD2").getPgPointsValue()).isEqualTo(200);
  }

  @Test
  void lookup_notFound_throwsRequiredFieldException() {
    when(repository.findAll()).thenReturn(List.of());
    service.refresh();

    assertThatThrownBy(() -> service.lookup("MISSING", "FAM1", "SUB1", "PROD1"))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("CfmPgPoints not found");
  }

  @Test
  void lookup_beforeRefresh_throwsRequiredFieldException() {
    assertThatThrownBy(() -> service.lookup("CFM001", "FAM1", "SUB1", "PROD1"))
        .isInstanceOf(RequiredFieldException.class);
  }

  @Test
  void refresh_replacesExistingCache() {
    when(repository.findAll())
        .thenReturn(List.of(entry("CFM001", "FAM1", "SUB1", "PROD1", 100)))
        .thenReturn(List.of(entry("CFM001", "FAM1", "SUB1", "PROD1", 999)));

    service.refresh();
    assertThat(service.lookup("CFM001", "FAM1", "SUB1", "PROD1").getPgPointsValue()).isEqualTo(100);

    service.refresh();
    assertThat(service.lookup("CFM001", "FAM1", "SUB1", "PROD1").getPgPointsValue()).isEqualTo(999);
  }
}
