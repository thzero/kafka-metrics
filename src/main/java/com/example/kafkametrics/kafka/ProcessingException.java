package com.example.kafkametrics.kafka;

import com.example.kafkametrics.deadletter.ReasonCode;

public class ProcessingException extends ReasonCodeException {

    public ProcessingException(String message, Throwable cause) {
        super(ReasonCode.PROCESSING_ERROR, message, cause);
    }

    public ProcessingException(String message) {
        super(ReasonCode.PROCESSING_ERROR, message);
    }
}
