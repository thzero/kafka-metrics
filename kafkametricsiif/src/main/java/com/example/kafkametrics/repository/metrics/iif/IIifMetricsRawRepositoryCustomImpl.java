package com.example.kafkametrics.repository.metrics.iif;

import com.example.kafkametrics.kafka.DatabaseException;
import com.example.kafkametrics.repository.metrics.EffectiveDateConstants;
import com.example.kafkametrics.util.JsonNodes;

import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

public class IIifMetricsRawRepositoryCustomImpl implements IIifMetricsRawRepositoryCustom {

    @PersistenceContext
    private EntityManager em;

    @Override
    @Transactional
    public void saveFromNode(String messageId, String agreementProductNbr, ObjectNode node) {
        Instant now = Instant.now();
        String assetId = JsonNodes.getText(node, "assetId").orElse(null);

        try {
            em.createQuery(
                    "UPDATE IifMetricsRaw r SET r.effEndDt = :now " +
                    "WHERE r.agreementProductNbr = :apn " +
                    "AND ((:assetId IS NULL AND r.assetId IS NULL) OR r.assetId = :assetId) " +
                    "AND r.effEndDt = :highDate")
                    .setParameter("now", now)
                    .setParameter("apn", agreementProductNbr)
                    .setParameter("assetId", assetId)
                    .setParameter("highDate", EffectiveDateConstants.HIGH_DATE)
                    .executeUpdate();

            IifMetricsRaw raw = new IifMetricsRaw();
            raw.setMessageId(messageId);
            raw.setAgreementProductNbr(agreementProductNbr);
            raw.setAssetId(assetId);
            raw.setAssetProductEntCd(JsonNodes.getText(node, "assetProductEntCd").orElse(null));
            raw.setProductFamilyEntCd(JsonNodes.getText(node, "productFamilyEntCd").orElse(null));
            raw.setProductSubFamilyEntCd(JsonNodes.getText(node, "productSubFamilyEntCd").orElse(null));
            raw.setPublishedDt(node.get("publishedDt").asLong());
            raw.setEffBeginDt(now);
            raw.setEffEndDt(EffectiveDateConstants.HIGH_DATE);
            em.persist(raw);
        } catch (Exception e) {
            throw new DatabaseException("Failed to persist IifMetricsRaw for messageId=" + messageId, e);
        }
    }
}
