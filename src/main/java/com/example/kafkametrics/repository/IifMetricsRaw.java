package com.example.kafkametrics.repository;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "iif_metrics_raw", indexes = {
        @Index(name = "idx_iif_metrics_raw_agreement_product_nbr", columnList = "agreement_product_nbr"),
        @Index(name = "idx_iif_metrics_raw_asset_id",              columnList = "asset_id")
})
public class IifMetricsRaw {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id")
    private String messageId;

    @Column(name = "agreement_product_nbr")
    private String agreementProductNbr;

    @Column(name = "asset_id")
    private String assetId;

    @Column(name = "asset_product_ent_cd")
    private String assetProductEntCd;

    @Column(name = "product_family_cd")
    private String productFamilyCd;

    @Column(name = "product_sub_family_cd")
    private String productSubFamilyCd;

    @Column(name = "processed_dt", nullable = false)
    private long processedDt;

    @Column(name = "eff_begin_dt", nullable = false)
    private Instant effBeginDt;

    @Column(name = "eff_end_dt", nullable = false)
    private Instant effEndDt;

    public Long getId() { return id; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getAgreementProductNbr() { return agreementProductNbr; }
    public void setAgreementProductNbr(String agreementProductNbr) { this.agreementProductNbr = agreementProductNbr; }

    public String getAssetId() { return assetId; }
    public void setAssetId(String assetId) { this.assetId = assetId; }

    public String getAssetProductEntCd() { return assetProductEntCd; }
    public void setAssetProductEntCd(String assetProductEntCd) { this.assetProductEntCd = assetProductEntCd; }

    public String getProductFamilyCd() { return productFamilyCd; }
    public void setProductFamilyCd(String productFamilyCd) { this.productFamilyCd = productFamilyCd; }

    public String getProductSubFamilyCd() { return productSubFamilyCd; }
    public void setProductSubFamilyCd(String productSubFamilyCd) { this.productSubFamilyCd = productSubFamilyCd; }

    public long getProcessedDt() { return processedDt; }
    public void setProcessedDt(long processedDt) { this.processedDt = processedDt; }

    public Instant getEffBeginDt() { return effBeginDt; }
    public void setEffBeginDt(Instant effBeginDt) { this.effBeginDt = effBeginDt; }

    public Instant getEffEndDt() { return effEndDt; }
    public void setEffEndDt(Instant effEndDt) { this.effEndDt = effEndDt; }
}
