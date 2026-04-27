package com.example.kafkametrics.repository;

import org.springframework.data.jpa.repository.JpaRepository;

public interface IIifMetricInclusionRepository extends JpaRepository<IifMetricInclusion, Long>, IIifMetricInclusionRepositoryCustom {
}
