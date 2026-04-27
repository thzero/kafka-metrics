package com.example.kafkametrics.processor.metrics;

import com.example.kafkametrics.kafka.KafkaProducerService;
import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.model.EventHeader;
import com.example.kafkametrics.util.JsonNodes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional
public class IifMetricsEventProcessor extends MetricsEventProcessor<JsonNode> {

    private static final Logger log = LoggerFactory.getLogger(IifMetricsEventProcessor.class);

    private final IifMetricsRawProcessorService iifMetricsRawProcessorService;
    private final IifMetricsIncludedProcessorService iifMetricsIncludedProcessorService;
    private final IifMetricsPgPointsProcessorService iifMetricsPgPointsProcessorService;

    public IifMetricsEventProcessor(KafkaProducerService publisher, ObjectMapper objectMapper,
                                       IifMetricsRawProcessorService iifMetricsRawProcessorService,
                                       IifMetricsIncludedProcessorService iifMetricsIncludedProcessorService,
                                       IifMetricsPgPointsProcessorService iifMetricsPgPointsProcessorService) {
        super(publisher, objectMapper);
        this.iifMetricsRawProcessorService = iifMetricsRawProcessorService;
        this.iifMetricsIncludedProcessorService = iifMetricsIncludedProcessorService;
        this.iifMetricsPgPointsProcessorService = iifMetricsPgPointsProcessorService;
    }

    @Override
    protected JsonNode processInternal(EventHeader incomingHeader, JsonNode payload, String messageId) {
        log.info("Processing IIF metrics message messageId={}", messageId);

        ObjectNode node = JsonNodes.asObjectNode(payload);

        String agreementProductNbr = JsonNodes.getText(node, "agreementProductNbr")
                .orElseThrow(() -> new RequiredFieldException("agreementProductNbr"));

        iifMetricsRawProcessorService.enrich(messageId, agreementProductNbr, node);
        iifMetricsIncludedProcessorService.process(messageId, agreementProductNbr, node);
        iifMetricsPgPointsProcessorService.process(messageId, agreementProductNbr, node);

        return node;
    }
}
