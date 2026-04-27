package com.example.kafkametrics.repository.lookup;

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

    @Column(name = "product_family_ent_cd")
    private String productFamilyEntCd;

    @Column(name = "product_sub_family_ent_cd")
    private String productSubFamilyEntCd;

    @Column(name = "asset_product_ent_cd")
    private String assetProductEntCd;

    @Column(name = "pg_points_value")
    private Integer pgPointsValue;

    public Long getId() { return id; }

    public String getCfmCd() { return cfmCd; }
    public void setCfmCd(String cfmCd) { this.cfmCd = cfmCd; }

    public String getProductFamilyEntCd() { return productFamilyEntCd; }
    public void setProductFamilyEntCd(String productFamilyEntCd) { this.productFamilyEntCd = productFamilyEntCd; }

    public String getProductSubFamilyEntCd() { return productSubFamilyEntCd; }
    public void setProductSubFamilyEntCd(String productSubFamilyEntCd) { this.productSubFamilyEntCd = productSubFamilyEntCd; }

    public String getAssetProductEntCd() { return assetProductEntCd; }
    public void setAssetProductEntCd(String assetProductEntCd) { this.assetProductEntCd = assetProductEntCd; }

    public Integer getPgPointsValue() { return pgPointsValue; }
    public void setPgPointsValue(Integer pgPointsValue) { this.pgPointsValue = pgPointsValue; }
}
