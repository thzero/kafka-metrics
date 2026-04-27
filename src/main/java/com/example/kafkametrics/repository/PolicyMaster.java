package com.example.kafkametrics.repository;

import jakarta.persistence.*;

import java.time.LocalDate;

@Entity
@Table(name = "policy_master", indexes = {
        @Index(name = "idx_policy_master_agreement_product_number", columnList = "agreement_product_number")
})
public class PolicyMaster {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "agreement_product_number")
    private String agreementProductNumber;

    @Column(name = "original_policy_effective_date")
    private LocalDate originalPolicyEffectiveDate;

    @Column(name = "scenario_cd")
    private String scenarioCd;

    public Long getId() { return id; }

    public String getAgreementProductNumber() { return agreementProductNumber; }
    public void setAgreementProductNumber(String agreementProductNumber) { this.agreementProductNumber = agreementProductNumber; }

    public LocalDate getOriginalPolicyEffectiveDate() { return originalPolicyEffectiveDate; }
    public void setOriginalPolicyEffectiveDate(LocalDate originalPolicyEffectiveDate) { this.originalPolicyEffectiveDate = originalPolicyEffectiveDate; }

    public String getScenarioCd() { return scenarioCd; }
    public void setScenarioCd(String scenarioCd) { this.scenarioCd = scenarioCd; }
}
