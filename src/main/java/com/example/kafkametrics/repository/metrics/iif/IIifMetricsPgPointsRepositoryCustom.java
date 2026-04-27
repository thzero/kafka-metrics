package com.example.kafkametrics.repository.metrics.iif;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface IIifMetricsPgPointsRepositoryCustom {

    void saveFromNode(String agreementProductNbr, ObjectNode node);
}
