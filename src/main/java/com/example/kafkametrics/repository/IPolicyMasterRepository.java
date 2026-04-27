package com.example.kafkametrics.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface IPolicyMasterRepository extends JpaRepository<PolicyMaster, Long> {

    List<PolicyMaster> findByAgreementProductNumber(String agreementProductNumber);
}
