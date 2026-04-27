package com.example.kafkametrics.kafka;

import com.example.kafkametrics.deadletter.ReasonCode;

public class DatabaseException extends ReasonCodeException {

    public DatabaseException(String message, Throwable cause) {
        super(ReasonCode.DATABASE_ERROR, message, cause);
    }
}
