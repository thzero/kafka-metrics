package com.example.kafkametrics.repository;

import com.example.kafkametrics.kafka.RequiredFieldException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class ProducerServiceImpl implements IProducerService {

    private static final Logger log = LoggerFactory.getLogger(ProducerServiceImpl.class);

    private final IProducerRepository producerRepository;

    public ProducerServiceImpl(IProducerRepository producerRepository) {
        this.producerRepository = producerRepository;
    }

    @Override
    @Cacheable("producer")
    public Producer findByAgencyNbr(String agencyNbr) {
        return producerRepository.findByAgencyNbr(agencyNbr).orElseThrow(() -> {
            log.warn("No Producer found for agencyNbr={}", agencyNbr);
            return new RequiredFieldException("Producer not found for agencyNbr=" + agencyNbr);
        });
    }
}
