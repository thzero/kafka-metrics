package com.example.kafkametrics.repository;

import com.example.kafkametrics.kafka.DatabaseException;
import com.example.kafkametrics.util.JsonNodes;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

public class IIifMetricsPgPointsRepositoryCustomImpl implements IIifMetricsPgPointsRepositoryCustom {

    @PersistenceContext
    private EntityManager em;

    @Override
    @Transactional
    public void saveFromNode(String agreementProductNbr, ObjectNode node) {
        Instant now = Instant.now();
        String assetId = JsonNodes.getText(node, "assetId").orElse(null);

        try {
            em.createQuery(
                    "UPDATE IifMetricPoints r SET r.effEndDt = :now " +
                    "WHERE r.agreementProductNbr = :apn " +
                    "AND ((:assetId IS NULL AND r.assetId IS NULL) OR r.assetId = :assetId) " +
                    "AND r.effEndDt = :highDate")
                    .setParameter("now", now)
                    .setParameter("apn", agreementProductNbr)
                    .setParameter("assetId", assetId)
                    .setParameter("highDate", EffectiveDateConstants.HIGH_DATE)
                    .executeUpdate();

            IifMetricsPgPoints points = new IifMetricsPgPoints();
            points.setAgreementProductNbr(agreementProductNbr);
            points.setAssetId(assetId);
            points.setProcessedDt(now);
            points.setPgPointsValue(JsonNodes.getInt(node, "pgPointsValue").orElse(null));
            points.setCfmCode(JsonNodes.getText(node, "cfmCode").orElse(null));
            points.setBonusPrimaryAgencyNbr(JsonNodes.getText(node, "bonusPrimaryAgencyNbr").orElse(null));
            points.setEffBeginDt(now);
            points.setEffEndDt(EffectiveDateConstants.HIGH_DATE);
            em.persist(points);
        } catch (Exception e) {
            throw new DatabaseException("Failed to persist IifMetricPoints for agreementProductNbr=" + agreementProductNbr, e);
        }
    }
}
