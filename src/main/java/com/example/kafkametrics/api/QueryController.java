package com.example.kafkametrics.api;

import com.example.kafkametrics.control.PublishedRecord;
import com.example.kafkametrics.control.IPublishedRecordRepository;
import com.example.kafkametrics.control.ReceivedRecord;
import com.example.kafkametrics.control.IReceivedRecordRepository;
import com.example.kafkametrics.deadletter.DeadLetterRecord;
import com.example.kafkametrics.deadletter.IDeadLetterRepository;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api")
public class QueryController {

    private final IReceivedRecordRepository receivedRecordRepository;
    private final IPublishedRecordRepository publishedRecordRepository;
    private final IDeadLetterRepository deadLetterRepository;

    public QueryController(IReceivedRecordRepository receivedRecordRepository,
                           IPublishedRecordRepository publishedRecordRepository,
                           IDeadLetterRepository deadLetterRepository) {
        this.receivedRecordRepository = receivedRecordRepository;
        this.publishedRecordRepository = publishedRecordRepository;
        this.deadLetterRepository = deadLetterRepository;
    }

    @GetMapping("/control/inbound")
    public List<ReceivedRecord> getInbound(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTimestamp,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTimestamp) {

        Instant from = TimeRangeHelper.resolveStart(startTimestamp);
        if (endTimestamp != null) {
            return receivedRecordRepository.findByReceivedAtBetween(from, endTimestamp);
        }
        return receivedRecordRepository.findByReceivedAtGreaterThanEqual(from);
    }

    @GetMapping("/control/outbound")
    public List<PublishedRecord> getOutbound(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTimestamp,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTimestamp) {

        Instant from = TimeRangeHelper.resolveStart(startTimestamp);
        if (endTimestamp != null) {
            return publishedRecordRepository.findByPublishedAtBetween(from, endTimestamp);
        }
        return publishedRecordRepository.findByPublishedAtGreaterThanEqual(from);
    }

    @GetMapping("/deadletter")
    public List<DeadLetterRecord> getDeadLetter(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTimestamp,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTimestamp) {

        Instant from = TimeRangeHelper.resolveStart(startTimestamp);
        if (endTimestamp != null) {
            return deadLetterRepository.findByFailedAtBetween(from, endTimestamp);
        }
        return deadLetterRepository.findByFailedAtGreaterThanEqual(from);
    }
}
