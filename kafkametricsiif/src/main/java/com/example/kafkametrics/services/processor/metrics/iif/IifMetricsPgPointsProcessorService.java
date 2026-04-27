package com.example.kafkametrics.services.processor.metrics.iif;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.CfmPgPoints;
import com.example.kafkametrics.repository.lookup.ICfmPgPointsRepository;
import com.example.kafkametrics.repository.lookup.Producer;
import com.example.kafkametrics.repository.metrics.iif.IIifMetricsPgPointsRepository;
import com.example.kafkametrics.services.metrics.lookup.IProducerService;
import com.example.kafkametrics.util.JsonNodes;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class IifMetricsPgPointsProcessorService {

    private static final Logger log = LoggerFactory.getLogger(IifMetricsPgPointsProcessorService.class);

    private final IIifMetricsPgPointsRepository pgPointsRepository;
    private final IProducerService producerService;
    private final ICfmPgPointsRepository cfmPgPointsRepository;

    public IifMetricsPgPointsProcessorService(IIifMetricsPgPointsRepository pgPointsRepository,
                                               IProducerService producerService,
                                               ICfmPgPointsRepository cfmPgPointsRepository) {
        this.pgPointsRepository = pgPointsRepository;
        this.producerService = producerService;
        this.cfmPgPointsRepository = cfmPgPointsRepository;
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

        CfmPgPoints cfm = lookupCfmPgPoints(cfmCd, productFamilyEntCd, productSubFamilyEntCd, assetProductEntCd);

        node.put("bonusPrimaryAgencyNbr", bonusPrimaryAgencyNbr);
        node.put("cfmCode", cfmCd);
        node.put("pgPointsValue", cfm.getPgPointsValue());

        node.put("processedDt", Instant.now().toEpochMilli());

        pgPointsRepository.saveFromNode(agreementProductNbr, node);
    }

    @Cacheable("cfmPgPoints")
    public CfmPgPoints lookupCfmPgPoints(String cfmCd, String productFamilyEntCd,
                                          String productSubFamilyEntCd, String assetProductEntCd) {
        return cfmPgPointsRepository
                .findByCfmCdAndProductFamilyEntCdAndProductSubFamilyEntCdAndAssetProductEntCd(
                        cfmCd, productFamilyEntCd, productSubFamilyEntCd, assetProductEntCd)
                .orElseThrow(() -> {
                    log.warn("No CfmPgPoints found for cfmCd={} productFamilyEntCd={} productSubFamilyEntCd={} assetProductEntCd={}",
                            cfmCd, productFamilyEntCd, productSubFamilyEntCd, assetProductEntCd);
                    return new RequiredFieldException("CfmPgPoints not found for cfmCd=" + cfmCd
                            + " productFamilyEntCd=" + productFamilyEntCd + " productSubFamilyEntCd=" + productSubFamilyEntCd
                            + " assetProductEntCd=" + assetProductEntCd);
                });
    }
}
