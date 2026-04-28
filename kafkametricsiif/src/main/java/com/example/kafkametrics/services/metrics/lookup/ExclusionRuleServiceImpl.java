package com.example.kafkametrics.services.metrics.lookup;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

/**
 * Evaluates whether a product should be excluded from IIF metric calculations.
 */
@Service
public class ExclusionRuleServiceImpl implements IExclusionRuleService {

    @Override
    public boolean isExcluded(ObjectNode node) {
        // TODO: ExclusionRuleServiceImpl is a stub that always returns false until the exclusion_rules
        //  entity and repository are built. Once implemented, this call will evaluate the node against
        //  the loaded ruleset and set excludedInd accordingly.
        return false;
    }
}
