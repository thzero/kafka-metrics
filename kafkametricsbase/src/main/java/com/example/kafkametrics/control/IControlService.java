package com.example.kafkametrics.control;

import org.springframework.dao.DataIntegrityViolationException;

public interface IControlService {

    // Inserts a ReceivedRecord. Throws DataIntegrityViolationException if messageId already exists
    // (unique constraint violation) — caller must catch this and route to dead letter as DUPLICATE.
    void recordReceived(String messageId, String interactionId) throws DataIntegrityViolationException;

    void recordPublished(String messageId, String interactionId);
}
