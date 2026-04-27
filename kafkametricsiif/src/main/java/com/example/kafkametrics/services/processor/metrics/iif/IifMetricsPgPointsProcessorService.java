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
        String productFamilyCd = JsonNodes.getText(node, "productFamilyCd")
                .orElseThrow(() -> new RequiredFieldException("productFamilyCd"));
        String productSubFamilyCd = JsonNodes.getText(node, "productSubFamilyCd")
                .orElseThrow(() -> new RequiredFieldException("productSubFamilyCd"));
        String assetProductCd = JsonNodes.getText(node, "assetProductCd")
                .orElseThrow(() -> new RequiredFieldException("assetProductCd"));

        Producer producer = producerService.findByAgencyNbr(agencyNbr);

        String cfmCd = producer.getCfmCd();
        String bonusPrimaryAgencyNbr = producer.getBonusPrimaryAgencyNbr();

        CfmPgPoints cfm = lookupCfmPgPoints(cfmCd, productFamilyCd, productSubFamilyCd, assetProductCd);

        node.put("bonusPrimaryAgencyNbr", bonusPrimaryAgencyNbr);
        node.put("cfmCode", cfmCd);
        node.put("pgPointsValue", cfm.getPgPointsValue());

        node.put("processedDt", Instant.now().toEpochMilli());

        pgPointsRepository.saveFromNode(agreementProductNbr, node);
    }

    @Cacheable("cfmPgPoints")
    public CfmPgPoints lookupCfmPgPoints(String cfmCd, String productFamilyCd,
                                          String productSubFamilyCd, String assetProductCd) {
        return cfmPgPointsRepository
                .findByCfmCdAndProductFamilyCdAndProductSubFamilyCdAndAssetProductCd(
                        cfmCd, productFamilyCd, productSubFamilyCd, assetProductCd)
                .orElseThrow(() -> {
                    log.warn("No CfmPgPoints found for cfmCd={} productFamilyCd={} productSubFamilyCd={} assetProductCd={}",
                            cfmCd, productFamilyCd, productSubFamilyCd, assetProductCd);
                    return new RequiredFieldException("CfmPgPoints not found for cfmCd=" + cfmCd
                            + " productFamilyCd=" + productFamilyCd + " productSubFamilyCd=" + productSubFamilyCd
                            + " assetProductCd=" + assetProductCd);
                });
    }
}
