package com.example.kafkametrics.repository;

import com.example.kafkametrics.kafka.RequiredFieldException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PolicyMasterServiceImpl implements IPolicyMasterService {

    private static final Logger log = LoggerFactory.getLogger(PolicyMasterServiceImpl.class);

    private final IPolicyMasterRepository policyMasterRepository;

    public PolicyMasterServiceImpl(IPolicyMasterRepository policyMasterRepository) {
        this.policyMasterRepository = policyMasterRepository;
    }

    @Override
    @Cacheable("policyMaster")
    public PolicyMaster findByAgreementProductNumber(String agreementProductNumber) {
        List<PolicyMaster> results = policyMasterRepository.findByAgreementProductNumber(agreementProductNumber);
        if (results.isEmpty()) {
            log.warn("No PolicyMaster found for agreementProductNumber={}", agreementProductNumber);
            throw new RequiredFieldException("PolicyMaster not found for agreementProductNumber=" + agreementProductNumber);
        }
        return results.get(0);
    }
}
