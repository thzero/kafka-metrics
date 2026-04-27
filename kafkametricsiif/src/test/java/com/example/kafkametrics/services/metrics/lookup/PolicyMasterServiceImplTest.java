package com.example.kafkametrics.services.metrics.lookup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.IPolicyMasterRepository;
import com.example.kafkametrics.repository.lookup.PolicyMaster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class PolicyMasterServiceImplTest {

  @Mock private IPolicyMasterRepository repository;

  private PolicyMasterServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new PolicyMasterServiceImpl(repository);
  }

  @Test
  void findByAgreementProductNumber_found_returnsRecord() {
    PolicyMaster pm = new PolicyMaster();
    pm.setAgreementProductNumber("AGR001");
    pm.setOriginalPolicyEffectiveDate(LocalDate.of(2020, 1, 1));
    pm.setScenarioCd("SC01");
    when(repository.findByAgreementProductNumber("AGR001")).thenReturn(List.of(pm));

    PolicyMaster result = service.findByAgreementProductNumber("AGR001");

    assertThat(result.getAgreementProductNumber()).isEqualTo("AGR001");
    assertThat(result.getScenarioCd()).isEqualTo("SC01");
  }

  @Test
  void findByAgreementProductNumber_multipleResults_returnsFirst() {
    PolicyMaster first = new PolicyMaster();
    first.setAgreementProductNumber("AGR001");
    first.setScenarioCd("FIRST");

    PolicyMaster second = new PolicyMaster();
    second.setAgreementProductNumber("AGR001");
    second.setScenarioCd("SECOND");

    when(repository.findByAgreementProductNumber("AGR001")).thenReturn(List.of(first, second));

    PolicyMaster result = service.findByAgreementProductNumber("AGR001");

    assertThat(result.getScenarioCd()).isEqualTo("FIRST");
  }

  @Test
  void findByAgreementProductNumber_notFound_throwsRequiredFieldException() {
    when(repository.findByAgreementProductNumber("MISSING")).thenReturn(List.of());

    assertThatThrownBy(() -> service.findByAgreementProductNumber("MISSING"))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("MISSING");
  }
}
