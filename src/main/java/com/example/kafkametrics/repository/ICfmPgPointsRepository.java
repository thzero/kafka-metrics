package com.example.kafkametrics.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ICfmPgPointsRepository extends JpaRepository<CfmPgPoints, Long> {

    Optional<CfmPgPoints> findByCfmCdAndProductFamilyCdAndProductSubFamilyCdAndAssetProductCd(
            String cfmCd,
            String productFamilyCd,
            String productSubFamilyCd,
            String assetProductCd);
}
