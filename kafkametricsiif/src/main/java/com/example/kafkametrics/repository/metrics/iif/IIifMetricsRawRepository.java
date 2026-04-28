package com.example.kafkametrics.repository.metrics.iif;

import org.springframework.data.jpa.repository.JpaRepository;

public interface IIifMetricsRawRepository extends JpaRepository<IifMetricsRaw, Long>, IIifMetricsRawRepositoryCustom {
}
