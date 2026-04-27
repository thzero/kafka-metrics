package com.example.kafkametrics.control;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface IReceivedRecordRepository extends JpaRepository<ReceivedRecord, Long> {

    boolean existsByMessageId(String messageId);

    List<ReceivedRecord> findByReceivedAtGreaterThanEqual(Instant from);

    List<ReceivedRecord> findByReceivedAtBetween(Instant from, Instant to);
}
