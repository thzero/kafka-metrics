package com.example.kafkametrics.repository;

import com.example.kafkametrics.kafka.DatabaseException;
import com.example.kafkametrics.util.JsonNodes;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

public class IIifMetricInclusionRepositoryCustomImpl implements IIifMetricInclusionRepositoryCustom {

    @PersistenceContext
    private EntityManager em;

    @Override
    @Transactional
    public void saveFromNode(String agreementProductNbr, boolean excludedInd, ObjectNode node) {
        Instant now = Instant.now();
        String assetId = JsonNodes.getText(node, "assetId").orElse(null);

        try {
            em.createQuery(
                    "UPDATE IifMetricInclusion r SET r.effEndDt = :now " +
                    "WHERE r.agreementProductNbr = :apn " +
                    "AND ((:assetId IS NULL AND r.assetId IS NULL) OR r.assetId = :assetId) " +
                    "AND r.effEndDt = :highDate")
                    .setParameter("now", now)
                    .setParameter("apn", agreementProductNbr)
                    .setParameter("assetId", assetId)
                    .setParameter("highDate", EffectiveDateConstants.HIGH_DATE)
                    .executeUpdate();

            IifMetricInclusion inclusion = new IifMetricInclusion();
            inclusion.setAgreementProductNbr(agreementProductNbr);
            inclusion.setAssetId(assetId);
            inclusion.setProcessedDt(now);
            inclusion.setExcludedInd(excludedInd);
            inclusion.setEffBeginDt(now);
            inclusion.setEffEndDt(EffectiveDateConstants.HIGH_DATE);
            em.persist(inclusion);
        } catch (Exception e) {
            throw new DatabaseException("Failed to persist IifMetricInclusion for agreementProductNbr=" + agreementProductNbr, e);
        }
    }
}
