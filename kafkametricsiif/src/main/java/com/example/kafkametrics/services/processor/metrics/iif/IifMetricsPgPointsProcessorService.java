package com.example.kafkametrics.services.processor.metrics.iif;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.CfmPgPoints;
import com.example.kafkametrics.repository.lookup.Producer;
import com.example.kafkametrics.repository.metrics.iif.IIifMetricsPgPointsRepository;
import com.example.kafkametrics.services.metrics.lookup.ICfmPgPointsService;
import com.example.kafkametrics.services.metrics.lookup.IProducerService;
import com.example.kafkametrics.util.JsonNodes;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IifMetricsPgPointsProcessorService {

    private static final Logger log = LoggerFactory.getLogger(IifMetricsPgPointsProcessorService.class);

    private final IIifMetricsPgPointsRepository pgPointsRepository;
    private final IProducerService producerService;
    private final ICfmPgPointsService cfmPgPointsService;

    public IifMetricsPgPointsProcessorService(IIifMetricsPgPointsRepository pgPointsRepository,
                                               IProducerService producerService,
                                               ICfmPgPointsService cfmPgPointsService) {
        this.pgPointsRepository = pgPointsRepository;
        this.producerService = producerService;
        this.cfmPgPointsService = cfmPgPointsService;
    }

    @Transactional
    public void process(String messageId, String agreementProductNbr, ObjectNode node) {
        log.info("Processing IIF metrics PG points messageId={} agreementProductNbr={}", messageId, agreementProductNbr);

        String agencyNbr = JsonNodes.getText(node, "agencyNbr")
                .orElseThrow(() -> new RequiredFieldException("agencyNbr"));
        String productFamilyEntCd = JsonNodes.getText(node, "productFamilyEntCd")
                .orElseThrow(() -> new RequiredFieldException("productFamilyEntCd"));
        String productSubFamilyEntCd = JsonNodes.getText(node, "productSubFamilyEntCd")
                .orElseThrow(() -> new RequiredFieldException("productSubFamilyEntCd"));
        String assetProductEntCd = JsonNodes.getText(node, "assetProductEntCd")
                .orElseThrow(() -> new RequiredFieldException("assetProductEntCd"));

        Producer producer = producerService.findByAgencyNbr(agencyNbr);

        String cfmCd = producer.getCfmCd();
        String bonusPrimaryAgencyNbr = producer.getBonusPrimaryAgencyNbr();

        CfmPgPoints cfm = cfmPgPointsService.lookup(cfmCd, productFamilyEntCd, productSubFamilyEntCd, assetProductEntCd);

        node.put("bonusPrimaryAgencyNbr", bonusPrimaryAgencyNbr);
        node.put("cfmCode", cfmCd);
        node.put("pgPointsValue", cfm.getPgPointsValue());

        pgPointsRepository.saveFromNode(agreementProductNbr, node);
    }
}
