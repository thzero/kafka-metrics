package com.example.kafkametrics.deadletter;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface IDeadLetterRepository extends JpaRepository<DeadLetterRecord, Long> {

    List<DeadLetterRecord> findByFailedAtGreaterThanEqual(Instant from);

    List<DeadLetterRecord> findByFailedAtBetween(Instant from, Instant to);
}
