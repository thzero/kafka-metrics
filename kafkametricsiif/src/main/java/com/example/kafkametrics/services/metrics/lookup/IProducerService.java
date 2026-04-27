package com.example.kafkametrics.services.metrics.lookup;

import com.example.kafkametrics.repository.lookup.Producer;

public interface IProducerService {

    Producer findByAgencyNbr(String agencyNbr);
}
