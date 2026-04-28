package com.example.kafkametrics.services.processor.metrics.iif;

import com.example.kafkametrics.repository.metrics.iif.IIifMetricInclusionRepository;
import com.example.kafkametrics.services.metrics.lookup.IExclusionRuleService;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IifMetricsIncludedProcessorService {

    private static final Logger log = LoggerFactory.getLogger(IifMetricsIncludedProcessorService.class);

    private final IIifMetricInclusionRepository inclusionRepository;
    private final IExclusionRuleService exclusionRuleService;

    public IifMetricsIncludedProcessorService(IIifMetricInclusionRepository inclusionRepository,
                                               IExclusionRuleService exclusionRuleService) {
        this.inclusionRepository = inclusionRepository;
        this.exclusionRuleService = exclusionRuleService;
    }

    @Transactional
    public void process(String messageId, String agreementProductNbr, ObjectNode node) {
        log.info("Processing IIF metrics included messageId={} agreementProductNbr={}", messageId, agreementProductNbr);

        boolean excludedInd = exclusionRuleService.isExcluded(node);

        node.put("excludedInd", excludedInd);

        inclusionRepository.saveFromNode(agreementProductNbr, excludedInd, node);
    }
}
