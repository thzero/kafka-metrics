package com.example.kafkametrics.kafka;

import com.example.kafkametrics.deadletter.ReasonCode;

public abstract class ReasonCodeException extends RuntimeException {

    private final ReasonCode reasonCode;

    protected ReasonCodeException(ReasonCode reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    protected ReasonCodeException(ReasonCode reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode;
    }

    public ReasonCode getReasonCode() {
        return reasonCode;
    }
}
