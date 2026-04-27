package com.example.kafkametrics.repository;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "iif_metric_inclusion", indexes = {
        @Index(name = "idx_iif_metric_inclusion_agreement_product_nbr", columnList = "agreement_product_nbr"),
        @Index(name = "idx_iif_metric_inclusion_asset_id",              columnList = "asset_id")
})
public class IifMetricInclusion {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "agreement_product_nbr")
    private String agreementProductNbr;

    @Column(name = "asset_id")
    private String assetId;

    @Column(name = "processed_dt", nullable = false)
    private Instant processedDt;

    @Column(name = "excluded_ind", nullable = false)
    private boolean excludedInd;

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

    public boolean isExcludedInd() { return excludedInd; }
    public void setExcludedInd(boolean excludedInd) { this.excludedInd = excludedInd; }

    public Instant getEffBeginDt() { return effBeginDt; }
    public void setEffBeginDt(Instant effBeginDt) { this.effBeginDt = effBeginDt; }

    public Instant getEffEndDt() { return effEndDt; }
    public void setEffEndDt(Instant effEndDt) { this.effEndDt = effEndDt; }
}
