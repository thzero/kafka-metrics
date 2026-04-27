package com.example.kafkametrics.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Seeds {@link PolicyMaster} and {@link PolicyAor} with fake data on startup.
 * Enabled via {@code app.seed-data.enabled=true} in application.yml.
 */
@Component
@ConditionalOnProperty(name = "app.seed-data.enabled", havingValue = "true")
public class IifDataSeeder implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(IifDataSeeder.class);

    private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final String[] SCENARIO_CODES = { "NEW", "RENEWAL", "REWRITE" };

    private static final int POLICY_COUNT = 1000;
    private static final int AOR_COUNT = 250;
    private static final int PRODUCER_PRIMARY_COUNT = AOR_COUNT / 3; // ~83 primaries

    private final IPolicyMasterRepository policyMasterRepository;
    private final IPolicyAorRepository policyAorRepository;
    private final IProducerRepository producerRepository;
    private final ICfmPgPointsRepository cfmPgPointsRepository;
    private final Random random = new Random(42);

    public IifDataSeeder(IPolicyMasterRepository policyMasterRepository,
                         IPolicyAorRepository policyAorRepository,
                         IProducerRepository producerRepository,
                         ICfmPgPointsRepository cfmPgPointsRepository) {
        this.policyMasterRepository = policyMasterRepository;
        this.policyAorRepository = policyAorRepository;
        this.producerRepository = producerRepository;
        this.cfmPgPointsRepository = cfmPgPointsRepository;
    }

    @Override
    public void run(String... args) {
        log.info("[SEED] Seeding {} PolicyMaster and {} PolicyAor records...", POLICY_COUNT, AOR_COUNT);

        List<PolicyMaster> policies = new ArrayList<>(POLICY_COUNT);
        for (int i = 0; i < POLICY_COUNT; i++) {
            PolicyMaster pm = new PolicyMaster();
            pm.setAgreementProductNumber(randomAlphanumeric(15));
            pm.setOriginalPolicyEffectiveDate(randomDate(
                    LocalDate.of(2015, 1, 1), LocalDate.of(2025, 12, 31)));
            pm.setScenarioCd(SCENARIO_CODES[random.nextInt(SCENARIO_CODES.length)]);
            policies.add(pm);
        }
        policyMasterRepository.saveAll(policies);

        // Select AOR_COUNT distinct policies at random and create one AOR entry each
        List<PolicyMaster> shuffled = new ArrayList<>(policies);
        Collections.shuffle(shuffled, random);

        List<PolicyAor> aors = new ArrayList<>(AOR_COUNT);
        for (PolicyMaster pm : shuffled.subList(0, AOR_COUNT)) {
            PolicyAor aor = new PolicyAor();
            aor.setAgreementProductNumber(pm.getAgreementProductNumber());
            aor.setAgencyNbr("0" + randomAlphanumeric(5));
            aor.setAssigned(random.nextBoolean());
            aors.add(aor);
        }
        policyAorRepository.saveAll(aors);

        // Collect all seeded agencyNbrs, shuffle, and split into primaries (~1/3) and members (~2/3).
        // Each member maps to one of the primaries as its bonusPrimaryAgencyNbr.
        List<String> agencyNbrs = aors.stream().map(PolicyAor::getAgencyNbr).collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        Collections.shuffle(agencyNbrs, random);
        List<String> primaries = agencyNbrs.subList(0, PRODUCER_PRIMARY_COUNT);
        List<String> members   = agencyNbrs.subList(PRODUCER_PRIMARY_COUNT, agencyNbrs.size());

        List<Producer> producers = new ArrayList<>(agencyNbrs.size());

        // Assign a unique cfmCd to each primary (CFM0001, CFM0002, ...)
        Map<String, String> primaryCfmMap = new HashMap<>();
        for (int i = 0; i < primaries.size(); i++) {
            primaryCfmMap.put(primaries.get(i), String.format("CFM%04d", i + 1));
        }

        // Primaries map to themselves
        for (String agencyNbr : primaries) {
            Producer p = new Producer();
            p.setAgencyNbr(agencyNbr);
            p.setBonusPrimaryAgencyNbr(agencyNbr);
            p.setCfmCd(primaryCfmMap.get(agencyNbr));
            producers.add(p);
        }

        // Members each map to a random primary
        for (String agencyNbr : members) {
            String bonusPrimary = primaries.get(random.nextInt(primaries.size()));
            Producer p = new Producer();
            p.setAgencyNbr(agencyNbr);
            p.setBonusPrimaryAgencyNbr(bonusPrimary);
            p.setCfmCd(primaryCfmMap.get(bonusPrimary));
            producers.add(p);
        }
        producerRepository.saveAll(producers);

        // Seed cfm_pg_points: one row per cfmCd x product combo, pgPointsValue in {10,15,...,50}
        int[] pgPointChoices = { 10, 15, 20, 25, 30, 35, 40, 45, 50 };
        String[][] combos = {
                { "transport", "auto", "auto"    },
                { "transport", "auto", "trailer" },
                { "dwelling",  "home", "home"    },
                { "dwelling",  "home", "condo"   },
                { "dwelling",  "home", "renters" }
        };
        List<CfmPgPoints> cfmRows = new ArrayList<>(primaryCfmMap.size() * combos.length);
        for (String cfmCd : primaryCfmMap.values()) {
            for (String[] combo : combos) {
                CfmPgPoints row = new CfmPgPoints();
                row.setCfmCd(cfmCd);
                row.setProductFamilyCd(combo[0]);
                row.setProductSubFamilyCd(combo[1]);
                row.setAssetProductCd(combo[2]);
                row.setPgPointsValue(pgPointChoices[random.nextInt(pgPointChoices.length)]);
                cfmRows.add(row);
            }
        }
        cfmPgPointsRepository.saveAll(cfmRows);

        log.info("[SEED] Done — {} policies, {} AOR records, {} producer mappings ({} primaries), {} cfm rows seeded",
                POLICY_COUNT, AOR_COUNT, producers.size(), primaries.size(), cfmRows.size());
    }

    private String randomAlphanumeric(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
        }
        return sb.toString();
    }

    private LocalDate randomDate(LocalDate from, LocalDate to) {
        long fromDay = from.toEpochDay();
        long toDay = to.toEpochDay();
        return LocalDate.ofEpochDay(fromDay + random.nextLong(toDay - fromDay));
    }
}
