package com.example.kafkametrics.services.processor.metrics.iif;

import com.example.kafkametrics.repository.metrics.iif.IIifMetricInclusionRepository;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class IifMetricsIncludedProcessorService {

    private static final Logger log = LoggerFactory.getLogger(IifMetricsIncludedProcessorService.class);

    private final IIifMetricInclusionRepository inclusionRepository;

    public IifMetricsIncludedProcessorService(IIifMetricInclusionRepository inclusionRepository) {
        this.inclusionRepository = inclusionRepository;
    }

    @Transactional
    public void process(String messageId, String agreementProductNbr, ObjectNode node) {
        log.info("Processing IIF metrics included messageId={} agreementProductNbr={}", messageId, agreementProductNbr);

        boolean excludedInd = false;

        node.put("excludedInd", excludedInd);
        node.put("processedDt", Instant.now().toEpochMilli());

        inclusionRepository.saveFromNode(agreementProductNbr, excludedInd, node);
    }
}
