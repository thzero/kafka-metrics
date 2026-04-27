package com.example.kafkametrics.repository;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "iif_metric_points", indexes = {
        @Index(name = "idx_iif_metric_points_agreement_product_nbr", columnList = "agreement_product_nbr"),
        @Index(name = "idx_iif_metric_points_asset_id",              columnList = "asset_id")
})
public class IifMetricsPgPoints {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "agreement_product_nbr")
    private String agreementProductNbr;

    @Column(name = "asset_id")
    private String assetId;

    @Column(name = "processed_dt", nullable = false)
    private Instant processedDt;

    @Column(name = "pg_points_value")
    private Integer pgPointsValue;

    @Column(name = "cfm_code")
    private String cfmCode;

    @Column(name = "bonus_primary_agency_nbr")
    private String bonusPrimaryAgencyNbr;

    @Column(name = "eff_begin_dt", nullable = false)
    private Instant effBeginDt;

    @Column(name = "eff_end_dt", nullable = false)
    private Instant effEndDt;

    public Long getId() { return id; }

    public String getAgreementProductNbr() { return agreementProductNbr; }
    public void setAgreementProductNbr(String agreementProductNbr) { this.agreementProductNbr = agreementProductNbr; }

    public String getAssetId() { return assetId; }
    public void setAssetId(String assetId) { this.assetId = assetId; }

    public Instant getProcessedDt() { return processedDt; }
    public void setProcessedDt(Instant processedDt) { this.processedDt = processedDt; }

    public Integer getPgPointsValue() { return pgPointsValue; }
    public void setPgPointsValue(Integer pgPointsValue) { this.pgPointsValue = pgPointsValue; }

    public String getCfmCode() { return cfmCode; }
    public void setCfmCode(String cfmCode) { this.cfmCode = cfmCode; }

    public String getBonusPrimaryAgencyNbr() { return bonusPrimaryAgencyNbr; }
    public void setBonusPrimaryAgencyNbr(String bonusPrimaryAgencyNbr) { this.bonusPrimaryAgencyNbr = bonusPrimaryAgencyNbr; }

    public Instant getEffBeginDt() { return effBeginDt; }
    public void setEffBeginDt(Instant effBeginDt) { this.effBeginDt = effBeginDt; }

    public Instant getEffEndDt() { return effEndDt; }
    public void setEffEndDt(Instant effEndDt) { this.effEndDt = effEndDt; }
}
