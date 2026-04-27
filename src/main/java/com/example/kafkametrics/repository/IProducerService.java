package com.example.kafkametrics.repository;

public interface IProducerService {

    Producer findByAgencyNbr(String agencyNbr);
}
