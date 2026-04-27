package com.example.kafkametrics.repository.metrics.iif;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface IIifMetricsRawRepositoryCustom {

    void saveFromNode(String messageId, String agreementProductNbr, ObjectNode node);
}
