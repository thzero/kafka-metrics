package com.example.kafkametrics.repository.lookup;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface IProducerRepository extends JpaRepository<Producer, Long> {

    Optional<Producer> findByAgencyNbr(String agencyNbr);
}
