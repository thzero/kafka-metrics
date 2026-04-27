package com.example.kafkametrics.deadletter;

public enum ReasonCode {
    INVALID_MESSAGE_ID,
    MISSING_PAYLOAD,
    DESERIALIZATION_ERROR,
    LISTENING_ERROR,
    PROCESSING_ERROR,
    PUBLISH_ERROR,
    CONTROL_RECORD_ERROR,
    DATABASE_ERROR,
    DUPLICATE
}
