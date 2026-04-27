package com.example.kafkametrics.repository;

import com.example.kafkametrics.kafka.RequiredFieldException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PolicyAorServiceImpl implements IPolicyAorService {

    private static final Logger log = LoggerFactory.getLogger(PolicyAorServiceImpl.class);

    private final IPolicyAorRepository policyAorRepository;

    public PolicyAorServiceImpl(IPolicyAorRepository policyAorRepository) {
        this.policyAorRepository = policyAorRepository;
    }

    @Override
    @Cacheable("policyAor")
    public PolicyAor findByAgreementProductNumber(String agreementProductNumber) {
        List<PolicyAor> results = policyAorRepository.findByAgreementProductNumber(agreementProductNumber);
        if (results.isEmpty()) {
            log.warn("No PolicyAor found for agreementProductNumber={}", agreementProductNumber);
            throw new RequiredFieldException("PolicyAor not found for agreementProductNumber=" + agreementProductNumber);
        }
        return results.get(0);
    }
}
