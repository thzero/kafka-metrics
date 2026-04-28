package com.example.kafkametrics.services.metrics.lookup;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.CfmPgPoints;
import com.example.kafkametrics.repository.lookup.ICfmPgPointsRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
public class CfmPgPointsServiceImpl implements ICfmPgPointsService {

    private static final Logger log = LoggerFactory.getLogger(CfmPgPointsServiceImpl.class);

    private record CfmPgPointsKey(String cfmCd, String productFamilyEntCd,
                                   String productSubFamilyEntCd, String assetProductEntCd) {}

    private final AtomicReference<Map<CfmPgPointsKey, CfmPgPoints>> cache =
            new AtomicReference<>(Map.of());

    private final ICfmPgPointsRepository repository;

    public CfmPgPointsServiceImpl(ICfmPgPointsRepository repository) {
        this.repository = repository;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        refresh();
    }

    @Override
    @Scheduled(fixedDelayString = "${app.cache.cfm-pg-points.refresh-interval-ms:900000}",
               initialDelayString = "${app.cache.cfm-pg-points.refresh-interval-ms:900000}")
    public void refresh() {
        Map<CfmPgPointsKey, CfmPgPoints> map = repository.findAll().stream()
                .collect(Collectors.toUnmodifiableMap(
                        r -> new CfmPgPointsKey(r.getCfmCd(), r.getProductFamilyEntCd(),
                                r.getProductSubFamilyEntCd(), r.getAssetProductEntCd()),
                        r -> r
                ));
        cache.set(map);
        log.info("CfmPgPoints cache refreshed: {} entries", map.size());
    }

    @Override
    public CfmPgPoints lookup(String cfmCd, String productFamilyEntCd,
                               String productSubFamilyEntCd, String assetProductEntCd) {
        CfmPgPointsKey key = new CfmPgPointsKey(cfmCd, productFamilyEntCd,
                productSubFamilyEntCd, assetProductEntCd);
        CfmPgPoints result = cache.get().get(key);
        if (result == null) {
            log.warn("No CfmPgPoints found for cfmCd={} productFamilyEntCd={} productSubFamilyEntCd={} assetProductEntCd={}",
                    cfmCd, productFamilyEntCd, productSubFamilyEntCd, assetProductEntCd);
            throw new RequiredFieldException("CfmPgPoints not found for cfmCd=" + cfmCd
                    + " productFamilyEntCd=" + productFamilyEntCd
                    + " productSubFamilyEntCd=" + productSubFamilyEntCd
                    + " assetProductEntCd=" + assetProductEntCd);
        }
        return result;
    }
}
