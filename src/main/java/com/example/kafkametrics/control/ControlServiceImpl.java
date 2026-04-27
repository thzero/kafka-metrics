package com.example.kafkametrics.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class ControlServiceImpl implements IControlService {

    private static final Logger log = LoggerFactory.getLogger(ControlServiceImpl.class);

    private final IReceivedRecordRepository receivedRepository;
    private final IPublishedRecordRepository publishedRepository;

    public ControlServiceImpl(IReceivedRecordRepository receivedRepository,
                               IPublishedRecordRepository publishedRepository) {
        this.receivedRepository = receivedRepository;
        this.publishedRepository = publishedRepository;
    }

    @Override
    @Transactional
    public void recordReceived(String messageId, String interactionId) {
        ReceivedRecord record = new ReceivedRecord();
        record.setMessageId(messageId);
        record.setInteractionId(interactionId);
        record.setReceivedAt(Instant.now());
        // Will throw DataIntegrityViolationException if messageId already exists.
        // The unique constraint is the duplicate guard — no separate SELECT needed.
        receivedRepository.saveAndFlush(record);
        log.info("Control record written: RECEIVED messageId={}", messageId);
    }

    @Override
    @Transactional
    public void recordPublished(String messageId, String interactionId) {
        PublishedRecord record = new PublishedRecord();
        record.setMessageId(messageId);
        record.setInteractionId(interactionId);
        record.setPublishedAt(Instant.now());
        publishedRepository.save(record);
        log.info("Control record written: PUBLISHED messageId={}", messageId);
    }
}
