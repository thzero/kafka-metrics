package com.example.kafkametrics.repository;

import org.springframework.data.jpa.repository.JpaRepository;

public interface IIifMetricsPgPointsRepository extends JpaRepository<IifMetricsPgPoints, Long>, IIifMetricsPgPointsRepositoryCustom {
}
