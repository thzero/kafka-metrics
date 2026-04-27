package com.example.kafkametrics.repository;

import jakarta.persistence.*;

@Entity
@Table(name = "cfm_pg_points", indexes = {
        @Index(name = "idx_cfm_pg_points_cfm_cd", columnList = "cfm_cd")
})
public class CfmPgPoints {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "cfm_cd")
    private String cfmCd;

    @Column(name = "asset_product_ent_cd")
    private String assetProductEntCd;

    @Column(name = "product_family_cd")
    private String productFamilyCd;

    @Column(name = "product_sub_family_cd")
    private String productSubFamilyCd;

    @Column(name = "asset_product_cd")
    private String assetProductCd;

    @Column(name = "pg_points_value")
    private Integer pgPointsValue;

    public Long getId() { return id; }

    public String getCfmCd() { return cfmCd; }
    public void setCfmCd(String cfmCd) { this.cfmCd = cfmCd; }

    public String getAssetProductEntCd() { return assetProductEntCd; }
    public void setAssetProductEntCd(String assetProductEntCd) { this.assetProductEntCd = assetProductEntCd; }

    public String getProductFamilyCd() { return productFamilyCd; }
    public void setProductFamilyCd(String productFamilyCd) { this.productFamilyCd = productFamilyCd; }

    public String getProductSubFamilyCd() { return productSubFamilyCd; }
    public void setProductSubFamilyCd(String productSubFamilyCd) { this.productSubFamilyCd = productSubFamilyCd; }

    public String getAssetProductCd() { return assetProductCd; }
    public void setAssetProductCd(String assetProductCd) { this.assetProductCd = assetProductCd; }

    public Integer getPgPointsValue() { return pgPointsValue; }
    public void setPgPointsValue(Integer pgPointsValue) { this.pgPointsValue = pgPointsValue; }
}
