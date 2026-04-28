package com.example.kafkametrics.repository.lookup;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface IPolicyAorRepository extends JpaRepository<PolicyAor, Long> {

    Optional<PolicyAor> findByAgreementProductNumber(String agreementProductNumber);
}
