package com.example.kafkametrics.repository;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface IIifMetricInclusionRepositoryCustom {

    void saveFromNode(String agreementProductNbr, boolean excludedInd, ObjectNode node);
}
