package com.example.kafkametrics.util;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

public final class JsonNodes {

    private JsonNodes() {}

    public static ObjectNode asObjectNode(JsonNode node) {
        if (!(node instanceof ObjectNode on)) throw new RequiredFieldException("payload must be a JSON object");
        return on;
    }

    public static Optional<String> getText(ObjectNode node, String field) {
        JsonNode n = node.get(field);
        return (n == null || n.isNull()) ? Optional.empty() : Optional.of(n.asText());
    }

    public static Optional<Integer> getInt(ObjectNode node, String field) {
        JsonNode n = node.get(field);
        return (n == null || n.isNull()) ? Optional.empty() : Optional.of(n.asInt());
    }
}
