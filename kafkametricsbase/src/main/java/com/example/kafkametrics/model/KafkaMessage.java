package com.example.kafkametrics.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public record KafkaMessage(
        @JsonProperty("header")  EventHeader header,
        @JsonProperty("payload") JsonNode payload) {
}
