package com.example.kafkametrics.services.metrics.lookup;

import com.example.kafkametrics.kafka.RequiredFieldException;
import com.example.kafkametrics.repository.lookup.IPolicyMasterRepository;
import com.example.kafkametrics.repository.lookup.PolicyMaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

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
        return policyMasterRepository.findByAgreementProductNumber(agreementProductNumber).orElseThrow(() -> {
            log.warn("No PolicyMaster found for agreementProductNumber={}", agreementProductNumber);
            return new RequiredFieldException("PolicyMaster not found for agreementProductNumber=" + agreementProductNumber);
        });
    }
}
