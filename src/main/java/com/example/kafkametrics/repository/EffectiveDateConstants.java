package com.example.kafkametrics.repository;

import java.time.Instant;

public final class EffectiveDateConstants {

    /** High date sentinel — marks the currently active (open-ended) record. */
    public static final Instant HIGH_DATE = Instant.ofEpochMilli(1231999900000L);

    private EffectiveDateConstants() {}
}
