package com.example.kafkametrics.services.metrics.lookup;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface IExclusionRuleService {

    boolean isExcluded(ObjectNode node);
}
