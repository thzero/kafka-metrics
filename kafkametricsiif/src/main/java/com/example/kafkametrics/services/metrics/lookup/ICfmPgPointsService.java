package com.example.kafkametrics.services.metrics.lookup;

import com.example.kafkametrics.repository.lookup.CfmPgPoints;

public interface ICfmPgPointsService {

    CfmPgPoints lookup(String cfmCd, String productFamilyEntCd,
                       String productSubFamilyEntCd, String assetProductEntCd);

    void refresh();
}
