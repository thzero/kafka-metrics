package com.example.kafkametrics.repository.metrics.iif;

public interface PgPointsAggregate {

    String getAssetProductEntCd();

    String getAgencyNbr();

    Long getTotalPgPoints();
}
