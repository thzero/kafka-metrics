package com.example.kafkametrics.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

class TimeRangeHelperTest {

  @Test
  void resolveStart_nullInput_returnsApproximately12HoursAgo() {
    Instant before = Instant.now().minus(12, ChronoUnit.HOURS).minusSeconds(1);
    Instant after = Instant.now().minus(12, ChronoUnit.HOURS).plusSeconds(1);

    Instant result = TimeRangeHelper.resolveStart(null);

    assertThat(result).isAfter(before).isBefore(after);
  }

  @Test
  void resolveStart_providedInstant_returnsSameInstant() {
    Instant given = Instant.parse("2024-01-15T10:00:00Z");

    Instant result = TimeRangeHelper.resolveStart(given);

    assertThat(result).isEqualTo(given);
  }

  @Test
  void resolveStart_epochZero_returnsEpochZero() {
    Instant epoch = Instant.EPOCH;

    Instant result = TimeRangeHelper.resolveStart(epoch);

    assertThat(result).isEqualTo(epoch);
  }
}
