package com.example.kafkametrics.processor.metrics;

import com.example.kafkametrics.config.AppProperties;
import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.IIifMetricsRawRepository;
import com.example.kafkametrics.repository.IPolicyAorService;
import com.example.kafkametrics.repository.IPolicyMasterService;
import com.example.kafkametrics.repository.PolicyAor;
import com.example.kafkametrics.repository.PolicyMaster;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Enriches IIF metrics payloads with {@link PolicyMaster} and {@link PolicyAor} data keyed by
 * {@code agreementProductNbr}.
 *
 * <p>Caching is delegated to {@link IPolicyMasterService} and {@link IPolicyAorService} so that
 * lookups go through the Spring proxy and Caffeine actually stores the results.
 */
@Service
public class IifMetricsRawProcessorService {

    private static final Logger log = LoggerFactory.getLogger(IifMetricsRawProcessorService.class);

    private final IPolicyMasterService policyMasterService;
    private final IPolicyAorService policyAorService;
    private final IIifMetricsRawRepository iifMetricsRawRepository;
    private final long lookupTimeoutMs;

    public IifMetricsRawProcessorService(IPolicyMasterService policyMasterService,
                                          IPolicyAorService policyAorService,
                                          IIifMetricsRawRepository iifMetricsRawRepository,
                                          AppProperties appProperties) {
        this.policyMasterService = policyMasterService;
        this.policyAorService = policyAorService;
        this.iifMetricsRawRepository = iifMetricsRawRepository;
        this.lookupTimeoutMs = appProperties.getProcessing().getLookupTimeoutMs();
    }

    @Transactional
    public void enrich(String messageId, String agreementProductNbr, ObjectNode node) {
        CompletableFuture<PolicyMaster> policyFuture =
                CompletableFuture.supplyAsync(() -> policyMasterService.findByAgreementProductNumber(agreementProductNbr))
                        .orTimeout(lookupTimeoutMs, TimeUnit.MILLISECONDS);
        CompletableFuture<PolicyAor> aorFuture =
                CompletableFuture.supplyAsync(() -> policyAorService.findByAgreementProductNumber(agreementProductNbr))
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
    }
}
