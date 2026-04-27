package com.example.kafkametrics.repository;

public interface IPolicyMasterService {

    PolicyMaster findByAgreementProductNumber(String agreementProductNumber);
}
