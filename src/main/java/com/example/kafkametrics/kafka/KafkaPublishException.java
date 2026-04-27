package com.example.kafkametrics.kafka;

import com.example.kafkametrics.deadletter.ReasonCode;

public class KafkaPublishException extends ReasonCodeException {

    public KafkaPublishException(String message, Throwable cause) {
        super(ReasonCode.PUBLISH_ERROR, message, cause);
    }
}
