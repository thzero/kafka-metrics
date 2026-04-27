package com.example.kafkametrics.repository.lookup;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface IPolicyAorRepository extends JpaRepository<PolicyAor, Long> {

    List<PolicyAor> findByAgreementProductNumber(String agreementProductNumber);
}
