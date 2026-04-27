package com.example.kafkametrics.repository.metrics.iif;

import org.springframework.data.jpa.repository.JpaRepository;

public interface IIifMetricsPgPointsRepository extends JpaRepository<IifMetricsPgPoints, Long>, IIifMetricsPgPointsRepositoryCustom {
}
