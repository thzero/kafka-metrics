package com.example.kafkametrics.repository;

import jakarta.persistence.*;

@Entity
@Table(name = "producer", indexes = {
        @Index(name = "idx_producer_agency_nbr", columnList = "agency_nbr")
})
public class Producer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "agency_nbr")
    private String agencyNbr;

    @Column(name = "bonus_primary_agency_nbr")
    private String bonusPrimaryAgencyNbr;

    @Column(name = "cfm_cd")
    private String cfmCd;

    public Long getId() { return id; }

    public String getAgencyNbr() { return agencyNbr; }
    public void setAgencyNbr(String agencyNbr) { this.agencyNbr = agencyNbr; }

    public String getBonusPrimaryAgencyNbr() { return bonusPrimaryAgencyNbr; }
    public void setBonusPrimaryAgencyNbr(String bonusPrimaryAgencyNbr) { this.bonusPrimaryAgencyNbr = bonusPrimaryAgencyNbr; }

    public String getCfmCd() { return cfmCd; }
    public void setCfmCd(String cfmCd) { this.cfmCd = cfmCd; }
}
