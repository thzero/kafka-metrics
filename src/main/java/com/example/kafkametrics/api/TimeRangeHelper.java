package com.example.kafkametrics.api;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public final class TimeRangeHelper {

    private TimeRangeHelper() {
    }

    public static Instant resolveStart(Instant start) {
        return start != null ? start : Instant.now().minus(12, ChronoUnit.HOURS);
    }
}
