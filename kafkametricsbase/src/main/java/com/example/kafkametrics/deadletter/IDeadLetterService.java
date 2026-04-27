package com.example.kafkametrics.deadletter;

public interface IDeadLetterService {

    void handle(String rawPayload, ReasonCode reasonCode, String messageId, String interactionId);
}
