package com.example.kafkametrics.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record EventHeader(
        @JsonProperty("messageId")         String messageId,
        @JsonProperty("interactionId")     String interactionId,
        @JsonProperty("sourceSystemEntCd") String sourceSystemEntCd) {

    public EventHeader withSourceSystemEntCd(String value) {
        return new EventHeader(messageId, interactionId, value);
    }
}
