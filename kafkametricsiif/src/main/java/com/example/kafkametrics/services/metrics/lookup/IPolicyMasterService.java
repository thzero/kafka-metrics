package com.example.kafkametrics.services.metrics.lookup;

import com.example.kafkametrics.repository.lookup.PolicyMaster;

public interface IPolicyMasterService {

    PolicyMaster findByAgreementProductNumber(String agreementProductNumber);
}
