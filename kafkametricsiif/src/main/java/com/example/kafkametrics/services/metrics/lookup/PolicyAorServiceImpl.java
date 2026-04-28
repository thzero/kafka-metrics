package com.example.kafkametrics.services.metrics.lookup;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.IPolicyAorRepository;
import com.example.kafkametrics.repository.lookup.PolicyAor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

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
        return policyAorRepository.findByAgreementProductNumber(agreementProductNumber).orElseThrow(() -> {
            log.warn("No PolicyAor found for agreementProductNumber={}", agreementProductNumber);
            return new RequiredFieldException("PolicyAor not found for agreementProductNumber=" + agreementProductNumber);
        });
    }
}
