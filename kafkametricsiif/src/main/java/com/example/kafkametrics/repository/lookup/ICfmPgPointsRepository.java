package com.example.kafkametrics.repository.lookup;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ICfmPgPointsRepository extends JpaRepository<CfmPgPoints, Long> {

    Optional<CfmPgPoints> findByCfmCdAndProductFamilyEntCdAndProductSubFamilyEntCdAndAssetProductEntCd(
            String cfmCd,
            String productFamilyEntCd,
            String productSubFamilyEntCd,
            String assetProductEntCd);
}
