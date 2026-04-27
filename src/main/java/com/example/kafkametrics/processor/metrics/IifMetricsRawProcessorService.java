package com.example.kafkametrics.processor.metrics;

import com.example.kafkametrics.config.AppProperties;
import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.IIifMetricsRawRepository;
import com.example.kafkametrics.repository.IPolicyAorRepository;
import com.example.kafkametrics.repository.IPolicyMasterRepository;
import com.example.kafkametrics.repository.PolicyAor;
import com.example.kafkametrics.repository.PolicyMaster;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Enriches IIF metrics payloads with {@link PolicyMaster} and {@link PolicyAor} data keyed by
 * {@code agreementProductNbr}.
 *
 * <p>Results are cached via Caffeine (TTL 15 min, max 10 000 entries per cache) so repeated
 * messages for the same agreement hit memory only. Cache misses are stored as
 * {@code Optional.empty()} to prevent repeated DB queries for unknown keys.
 */
@Service
public class IifMetricsRawProcessorService {

    private static final Logger log = LoggerFactory.getLogger(IifMetricsRawProcessorService.class);

    private final IPolicyMasterRepository policyMasterRepository;
    private final IPolicyAorRepository policyAorRepository;
    private final IIifMetricsRawRepository iifMetricsRawRepository;
    private final IifMetricsIncludedProcessorService iifMetricsIncludedProcessorService;
    private final IifMetricsPgPointsProcessorService iifMetricsPgPointsProcessorService;
    private final long lookupTimeoutMs;

    public IifMetricsRawProcessorService(IPolicyMasterRepository policyMasterRepository,
                                          IPolicyAorRepository policyAorRepository,
                                          IIifMetricsRawRepository iifMetricsRawRepository,
                                          IifMetricsIncludedProcessorService iifMetricsIncludedProcessorService,
                                          IifMetricsPgPointsProcessorService iifMetricsPgPointsProcessorService,
                                          AppProperties appProperties) {
        this.policyMasterRepository = policyMasterRepository;
        this.policyAorRepository = policyAorRepository;
        this.iifMetricsRawRepository = iifMetricsRawRepository;
        this.iifMetricsIncludedProcessorService = iifMetricsIncludedProcessorService;
        this.iifMetricsPgPointsProcessorService = iifMetricsPgPointsProcessorService;
        this.lookupTimeoutMs = appProperties.getProcessing().getLookupTimeoutMs();
    }

    @Transactional
    public void enrich(String messageId, String agreementProductNbr, ObjectNode node) {
        CompletableFuture<PolicyMaster> policyFuture =
                CompletableFuture.supplyAsync(() -> lookupPolicy(agreementProductNbr))
                        .orTimeout(lookupTimeoutMs, TimeUnit.MILLISECONDS);
        CompletableFuture<PolicyAor> aorFuture =
                CompletableFuture.supplyAsync(() -> lookupAor(agreementProductNbr))
                        .orTimeout(lookupTimeoutMs, TimeUnit.MILLISECONDS);

        PolicyMaster pm = policyFuture.join();
        PolicyAor aor = aorFuture.join();

        if (pm.getOriginalPolicyEffectiveDate() == null)
            throw new RequiredFieldException("originalPolicyEffectiveDate");
        if (pm.getScenarioCd() == null)
            throw new RequiredFieldException("scenarioCd");
        if (aor.getAgencyNbr() == null)
            throw new RequiredFieldException("agencyNbr");
        if (aor.getAssigned() == null)
            throw new RequiredFieldException("assigned");

        node.put("originalPolicyEffectiveDate", pm.getOriginalPolicyEffectiveDate().toString());
        node.put("scenarioCd", pm.getScenarioCd());
        node.put("agencyNbr", aor.getAgencyNbr());
        node.put("assigned", aor.getAssigned());

        iifMetricsRawRepository.saveFromNode(messageId, agreementProductNbr, node);
        iifMetricsIncludedProcessorService.process(messageId, agreementProductNbr, node);
        iifMetricsPgPointsProcessorService.process(messageId, agreementProductNbr, node);
    }

    @Cacheable("policyMaster")
    public PolicyMaster lookupPolicy(String agreementProductNbr) {
        List<PolicyMaster> results = policyMasterRepository.findByAgreementProductNumber(agreementProductNbr);
        if (results.isEmpty()) {
            log.warn("No PolicyMaster found for agreementProductNbr={}", agreementProductNbr);
            throw new RequiredFieldException("PolicyMaster not found for agreementProductNbr=" + agreementProductNbr);
        }
        return results.get(0);
    }

    @Cacheable("policyAor")
    public PolicyAor lookupAor(String agreementProductNbr) {
        List<PolicyAor> results = policyAorRepository.findByAgreementProductNumber(agreementProductNbr);
        if (results.isEmpty()) {
            log.warn("No PolicyAor found for agreementProductNbr={}", agreementProductNbr);
            throw new RequiredFieldException("PolicyAor not found for agreementProductNbr=" + agreementProductNbr);
        }
        return results.get(0);
    }
}
