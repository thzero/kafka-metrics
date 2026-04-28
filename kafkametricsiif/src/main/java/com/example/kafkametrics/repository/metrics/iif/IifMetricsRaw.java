package com.example.kafkametrics.repository.metrics.iif;

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

    @Column(name = "product_family_ent_cd")
    private String productFamilyEntCd;

    @Column(name = "product_sub_family_ent_cd")
    private String productSubFamilyEntCd;

    @Column(name = "published_dt", nullable = false)
    private long publishedDt;

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

    public String getProductFamilyEntCd() { return productFamilyEntCd; }
    public void setProductFamilyEntCd(String productFamilyEntCd) { this.productFamilyEntCd = productFamilyEntCd; }

    public String getProductSubFamilyEntCd() { return productSubFamilyEntCd; }
    public void setProductSubFamilyEntCd(String productSubFamilyEntCd) { this.productSubFamilyEntCd = productSubFamilyEntCd; }

    public long getPublishedDt() { return publishedDt; }
    public void setPublishedDt(long publishedDt) { this.publishedDt = publishedDt; }

    public Instant getEffBeginDt() { return effBeginDt; }
    public void setEffBeginDt(Instant effBeginDt) { this.effBeginDt = effBeginDt; }

    public Instant getEffEndDt() { return effEndDt; }
    public void setEffEndDt(Instant effEndDt) { this.effEndDt = effEndDt; }
}
