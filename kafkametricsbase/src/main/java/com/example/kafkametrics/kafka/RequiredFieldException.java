package com.example.kafkametrics.kafka;

import com.example.kafkametrics.deadletter.ReasonCode;

public class RequiredFieldException extends ReasonCodeException {

    public RequiredFieldException(String field) {
        super(ReasonCode.PROCESSING_ERROR, "Required field missing or null: " + field);
    }
}
