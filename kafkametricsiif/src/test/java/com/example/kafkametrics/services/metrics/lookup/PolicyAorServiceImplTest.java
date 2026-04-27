package com.example.kafkametrics.services.metrics.lookup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.IPolicyAorRepository;
import com.example.kafkametrics.repository.lookup.PolicyAor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

@ExtendWith(MockitoExtension.class)
class PolicyAorServiceImplTest {

  @Mock private IPolicyAorRepository repository;

  private PolicyAorServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new PolicyAorServiceImpl(repository);
  }

  @Test
  void findByAgreementProductNumber_found_returnsRecord() {
    PolicyAor aor = new PolicyAor();
    aor.setAgreementProductNumber("AGR001");
    aor.setAgencyNbr("AGENCY01");
    aor.setAssigned(true);
    when(repository.findByAgreementProductNumber("AGR001")).thenReturn(List.of(aor));

    PolicyAor result = service.findByAgreementProductNumber("AGR001");

    assertThat(result.getAgencyNbr()).isEqualTo("AGENCY01");
    assertThat(result.getAssigned()).isTrue();
  }

  @Test
  void findByAgreementProductNumber_multipleResults_returnsFirst() {
    PolicyAor first = new PolicyAor();
    first.setAgencyNbr("FIRST");
    PolicyAor second = new PolicyAor();
    second.setAgencyNbr("SECOND");
    when(repository.findByAgreementProductNumber("AGR001")).thenReturn(List.of(first, second));

    PolicyAor result = service.findByAgreementProductNumber("AGR001");

    assertThat(result.getAgencyNbr()).isEqualTo("FIRST");
  }

  @Test
  void findByAgreementProductNumber_notFound_throwsRequiredFieldException() {
    when(repository.findByAgreementProductNumber("MISSING")).thenReturn(List.of());

    assertThatThrownBy(() -> service.findByAgreementProductNumber("MISSING"))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("MISSING");
  }
}
