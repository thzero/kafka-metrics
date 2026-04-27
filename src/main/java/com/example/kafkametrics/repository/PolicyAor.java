package com.example.kafkametrics.repository;

import jakarta.persistence.*;

@Entity
@Table(name = "policy_aor", indexes = {
        @Index(name = "idx_policy_aor_agreement_product_number", columnList = "agreement_product_number")
})
public class PolicyAor {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "agreement_product_number")
    private String agreementProductNumber;

    @Column(name = "agency_nbr")
    private String agencyNbr;

    @Column(name = "assigned")
    private Boolean assigned;

    public Long getId() { return id; }

    public String getAgreementProductNumber() { return agreementProductNumber; }
    public void setAgreementProductNumber(String agreementProductNumber) { this.agreementProductNumber = agreementProductNumber; }

    public String getAgencyNbr() { return agencyNbr; }
    public void setAgencyNbr(String agencyNbr) { this.agencyNbr = agencyNbr; }

    public Boolean getAssigned() { return assigned; }
    public void setAssigned(Boolean assigned) { this.assigned = assigned; }
}
