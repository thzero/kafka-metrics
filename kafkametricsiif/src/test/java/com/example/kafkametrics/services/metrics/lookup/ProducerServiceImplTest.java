package com.example.kafkametrics.services.metrics.lookup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.IProducerRepository;
import com.example.kafkametrics.repository.lookup.Producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class ProducerServiceImplTest {

  @Mock private IProducerRepository repository;

  private ProducerServiceImpl service;

  @BeforeEach
  void setUp() {
    service = new ProducerServiceImpl(repository);
  }

  @Test
  void findByAgencyNbr_found_returnsProducer() {
    Producer producer = new Producer();
    producer.setAgencyNbr("AGENCY01");
    producer.setCfmCd("CFM001");
    producer.setBonusPrimaryAgencyNbr("BONUS01");
    when(repository.findByAgencyNbr("AGENCY01")).thenReturn(Optional.of(producer));

    Producer result = service.findByAgencyNbr("AGENCY01");

    assertThat(result.getCfmCd()).isEqualTo("CFM001");
    assertThat(result.getBonusPrimaryAgencyNbr()).isEqualTo("BONUS01");
  }

  @Test
  void findByAgencyNbr_notFound_throwsRequiredFieldException() {
    when(repository.findByAgencyNbr("MISSING")).thenReturn(Optional.empty());

    assertThatThrownBy(() -> service.findByAgencyNbr("MISSING"))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("MISSING");
  }
}
