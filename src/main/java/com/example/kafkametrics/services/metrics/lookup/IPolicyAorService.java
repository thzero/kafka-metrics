package com.example.kafkametrics.services.metrics.lookup;

import com.example.kafkametrics.repository.lookup.PolicyAor;

public interface IPolicyAorService {

    PolicyAor findByAgreementProductNumber(String agreementProductNumber);
}
