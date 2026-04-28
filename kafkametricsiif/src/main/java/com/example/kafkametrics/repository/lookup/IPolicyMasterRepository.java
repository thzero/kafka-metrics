package com.example.kafkametrics.repository.lookup;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface IPolicyMasterRepository extends JpaRepository<PolicyMaster, Long> {

    Optional<PolicyMaster> findByAgreementProductNumber(String agreementProductNumber);
}
