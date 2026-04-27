package com.example.kafkametrics.control;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface IPublishedRecordRepository extends JpaRepository<PublishedRecord, Long> {

    boolean existsByMessageId(String messageId);

    List<PublishedRecord> findByPublishedAtGreaterThanEqual(Instant from);

    List<PublishedRecord> findByPublishedAtBetween(Instant from, Instant to);
}
