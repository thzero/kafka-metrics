package com.example.kafkametrics.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.kafkametrics.kafka.RequiredFieldException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class JsonNodesTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void asObjectNode_objectNode_returnsObjectNode() {
    ObjectNode node = mapper.createObjectNode();
    assertThat(JsonNodes.asObjectNode(node)).isSameAs(node);
  }

  @Test
  void asObjectNode_arrayNode_throwsRequiredFieldException() {
    assertThatThrownBy(() -> JsonNodes.asObjectNode(mapper.createArrayNode()))
        .isInstanceOf(RequiredFieldException.class)
        .hasMessageContaining("payload must be a JSON object");
  }

  @Test
  void getText_presentField_returnsValue() {
    ObjectNode node = mapper.createObjectNode();
    node.put("key", "value");
    assertThat(JsonNodes.getText(node, "key")).hasValue("value");
  }

  @Test
  void getText_missingField_returnsEmpty() {
    ObjectNode node = mapper.createObjectNode();
    assertThat(JsonNodes.getText(node, "missing")).isEmpty();
  }

  @Test
  void getText_nullField_returnsEmpty() {
    ObjectNode node = mapper.createObjectNode();
    node.putNull("key");
    assertThat(JsonNodes.getText(node, "key")).isEmpty();
  }

  @Test
  void getInt_presentField_returnsValue() {
    ObjectNode node = mapper.createObjectNode();
    node.put("count", 42);
    assertThat(JsonNodes.getInt(node, "count")).hasValue(42);
  }

  @Test
  void getInt_missingField_returnsEmpty() {
    ObjectNode node = mapper.createObjectNode();
    assertThat(JsonNodes.getInt(node, "missing")).isEmpty();
  }

  @Test
  void getInt_nullField_returnsEmpty() {
    ObjectNode node = mapper.createObjectNode();
    node.putNull("count");
    assertThat(JsonNodes.getInt(node, "count")).isEmpty();
  }
}
