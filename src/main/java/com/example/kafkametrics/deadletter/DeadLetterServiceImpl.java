package com.example.kafkametrics.deadletter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class DeadLetterServiceImpl implements IDeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterServiceImpl.class);

    private final IDeadLetterRepository repository;

    public DeadLetterServiceImpl(IDeadLetterRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional
    public void handle(String rawPayload, ReasonCode reasonCode, String messageId, String interactionId) {
        DeadLetterRecord record = new DeadLetterRecord();
        record.setRawPayload(rawPayload);
        record.setReasonCode(reasonCode);
        record.setMessageId(messageId);
        record.setInteractionId(interactionId);
        record.setFailedAt(Instant.now());
        repository.save(record);
        log.error("Dead letter routed: reasonCode={} messageId={}", reasonCode, messageId);
    }
}
