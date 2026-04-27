package com.example.kafkametrics.kafka;

import com.example.kafkametrics.control.IControlService;
import com.example.kafkametrics.deadletter.IDeadLetterService;
import com.example.kafkametrics.deadletter.ReasonCode;
import com.example.kafkametrics.logging.MdcContext;
import com.example.kafkametrics.model.KafkaMessage;
import com.example.kafkametrics.processor.IEventProcessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaConsumerListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerListener.class);

    private final ObjectMapper objectMapper;
    private final IControlService controlService;
    private final IEventProcessor processor;
    private final IDeadLetterService deadLetterService;
    private final ScheduledExecutorService processingScheduler;
    private final MeterRegistry meterRegistry;

    // In-memory set of messageIds currently in-flight (consumer thread → worker thread).
    // add() returns false if already present → duplicate detected in nanoseconds with no DB call.
    // Cleared in the worker's finally block so redelivered messages can re-enter the pipeline.
    private final Set<String> inFlightIds = ConcurrentHashMap.newKeySet();

    // Pre-cached Micrometer meters keyed by eventType or ReasonCode.
    // Avoids tag-array (String[]) allocation and registry map lookup on every message in the hot path.
    // computeIfAbsent registers the meter on first use; all subsequent calls are O(1) CHM get.
    private final ConcurrentHashMap<String, Counter> receivedCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> publishedCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> failedCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> e2eTimers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> pipelineTimers = new ConcurrentHashMap<>();

    @Value("${app.processing.status-log-interval-ms:10000}")
    private long statusLogIntervalMs;

    public KafkaConsumerListener(ObjectMapper objectMapper,
                                 IControlService controlService,
                                 IEventProcessor processor,
                                 IDeadLetterService deadLetterService,
                                 @Qualifier("processingScheduler") ScheduledExecutorService processingScheduler,
                                 MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.controlService = controlService;
        this.processor = processor;
        this.deadLetterService = deadLetterService;
        this.processingScheduler = processingScheduler;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    private void startStatusLogger() {
        Gauge.builder("kafka.processor.messages.in.flight", inFlightIds, Set::size)
                .description("Number of messages currently in-flight (scheduled + executing)")
                .register(meterRegistry);
        if (statusLogIntervalMs > 0) {
            processingScheduler.scheduleAtFixedRate(this::logStatus, statusLogIntervalMs, statusLogIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    // -------------------------------------------------------------------------
    // Main listener
    // -------------------------------------------------------------------------

    @KafkaListener(topics = "${kafka.topic.input}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        Optional<MessageContext> ctxOpt = deserializeAndValidate(record.value());
        if (ctxOpt.isEmpty()) return;

        MessageContext ctx = ctxOpt.get();
        MdcContext.set(ctx.interactionId(), ctx.messageId());
        try {
            log.info("[RECEIVED] eventType={} topic={} partition={} offset={}",
                    ctx.eventType(), record.topic(), record.partition(), record.offset());

            // In-memory duplicate gate — nanosecond cost, no DB round-trip.
            // Restart/replay duplicates are caught by the DB unique constraint inside processDeferred.
            if (!inFlightIds.add(ctx.messageId())) {
                log.warn("Duplicate messageId detected in-flight, routing to dead letter");
                deadLetter(ctx.rawPayload(), ReasonCode.DUPLICATE, ctx.messageId(), ctx.interactionId());
                acknowledgment.acknowledge();
                return;
            }

            // MDC snapshot so the worker thread can restore correlation IDs (MDC is thread-local).
            Map<String, String> mdcSnapshot = MDC.getCopyOfContextMap();
            // e2e timer starts here — after all fast-path exits — to capture delay + execution time.
            Timer.Sample e2eSample = Timer.start(meterRegistry);
            counter(receivedCounters, ctx.eventType(), "kafka.processor.messages.received").increment();

            try {
                processingScheduler.execute(
                        () -> processDeferred(ctx, acknowledgment, e2eSample, mdcSnapshot));
            } catch (Exception e) {
                // RejectedExecutionException (pool shutting down) — clean up before returning so
                // the inFlightIds slot does not leak (processDeferred will never run to clear it).
                log.error("[SUBMIT-FAIL] eventType={} -- failed to submit, routing to dead letter", ctx.eventType(), e);
                inFlightIds.remove(ctx.messageId());
                e2eSample.stop(timer(e2eTimers, ctx.eventType(), "kafka.processor.e2e.latency"));
                deadLetter(ctx.rawPayload(), ReasonCode.PROCESSING_ERROR, ctx.messageId(), ctx.interactionId());
                return;
            }
            log.info("[SUBMITTED] eventType={} inFlight={}", ctx.eventType(), inFlightIds.size());
        } catch (Exception e) {
            deadLetter(ctx.rawPayload(), ReasonCode.LISTENING_ERROR, ctx.messageId(), ctx.interactionId());
        } finally {
            MdcContext.clear();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers called from listen()
    // -------------------------------------------------------------------------

    /** Deserializes the payload and validates the messageId UUID. Returns empty and routes to DLQ on any failure. */
    private Optional<MessageContext> deserializeAndValidate(String rawPayload) {
        KafkaMessage message;
        try {
            message = objectMapper.readValue(rawPayload, KafkaMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Deserialization failed", e);
            deadLetter(rawPayload, ReasonCode.DESERIALIZATION_ERROR, null, null);
            return Optional.empty();
        }

        if (message.header() == null) {
            log.error("Missing header, routing to dead letter");
            deadLetter(rawPayload, ReasonCode.INVALID_MESSAGE_ID, null, null);
            return Optional.empty();
        }

        String interactionId = message.header().interactionId();
        String messageId = message.header().messageId();
        String eventType = message.header().eventType() != null ? message.header().eventType() : "unknown";

        if (message.payload() == null) {
            log.error("Missing payload, routing to dead letter: messageId={}", messageId);
            deadLetter(rawPayload, ReasonCode.MISSING_PAYLOAD, messageId, interactionId);
            return Optional.empty();
        }

        if (messageId == null || !isValidUuid(messageId)) {
            log.error("Invalid or missing messageId, routing to dead letter: messageId={}", messageId);
            deadLetter(rawPayload, ReasonCode.INVALID_MESSAGE_ID, messageId, interactionId);
            return Optional.empty();
        }

        return Optional.of(new MessageContext(rawPayload, message, messageId, interactionId, eventType));
    }

    // -------------------------------------------------------------------------
    // Worker (runs on scheduler thread)
    // -------------------------------------------------------------------------

    private void processDeferred(MessageContext ctx, Acknowledgment acknowledgment,
                                 Timer.Sample e2eSample, Map<String, String> mdcSnapshot) {
        if (mdcSnapshot != null) {
            MDC.setContextMap(mdcSnapshot);
        } else {
            MdcContext.set(ctx.interactionId(), ctx.messageId());
        }
        Timer.Sample pipelineSample = Timer.start(meterRegistry);
        try {
            log.info("[PROCESSING] eventType={}", ctx.eventType());

            try {
                controlService.recordReceived(ctx.messageId(), ctx.interactionId());
            } catch (DataIntegrityViolationException e) {
                // Row already exists — app restarted mid-flight; in-memory state was lost but DB row survived.
                log.warn("[DUPLICATE] eventType={} -- restart/replay duplicate, routing to dead letter", ctx.eventType());
                deadLetter(ctx.rawPayload(), ReasonCode.DUPLICATE, ctx.messageId(), ctx.interactionId());
                acknowledgment.acknowledge();
                return;
            } catch (Exception e) {
                log.error("Failed to write RECEIVED control record", e);
                deadLetter(ctx.rawPayload(), ReasonCode.CONTROL_RECORD_ERROR, ctx.messageId(), ctx.interactionId());
                return;
            }

            // Runs on a platform thread — no VT wrapping. VTs pin on Kafka's synchronized blocks,
            // exhausting the ForkJoinPool. Timeout enforced by delivery.timeout.ms on the producer.
            try {
                processor.process(ctx.message().header(), ctx.message().payload());
            } catch (ReasonCodeException e) {
                log.error("Processing failed reason={}", e.getReasonCode(), e);
                deadLetter(ctx.rawPayload(), e.getReasonCode(), ctx.messageId(), ctx.interactionId());
                return;
            } catch (Exception e) {
                log.error("Processing failed", e);
                deadLetter(ctx.rawPayload(), ReasonCode.PROCESSING_ERROR, ctx.messageId(), ctx.interactionId());
                return;
            }

            try {
                controlService.recordPublished(ctx.messageId(), ctx.interactionId());
            } catch (Exception e) {
                log.error("Failed to write PUBLISHED control record", e);
                deadLetter(ctx.rawPayload(), ReasonCode.CONTROL_RECORD_ERROR, ctx.messageId(), ctx.interactionId());
                return;
            }

            counter(publishedCounters, ctx.eventType(), "kafka.processor.messages.published").increment();
            acknowledgment.acknowledge();
            log.info("[PUBLISHED] eventType={}", ctx.eventType());

        } finally {
            pipelineSample.stop(timer(pipelineTimers, ctx.eventType(), "kafka.processor.pipeline.latency"));
            e2eSample.stop(timer(e2eTimers, ctx.eventType(), "kafka.processor.e2e.latency"));
            inFlightIds.remove(ctx.messageId()); // always release — success clears for replay, failure clears for redelivery
            MDC.clear(); // full clear — pool threads are reused; selective clear risks leaking keys
        }
    }

    private void logStatus() {
        int inFlight = inFlightIds.size();
        if (inFlight > 0) {
            log.info("[STATUS] inFlight={}", inFlight);
        } else {
            log.debug("[STATUS] idle — inFlight=0");
        }
    }

    // -------------------------------------------------------------------------
    // Meter cache helpers — avoid per-call String[] allocation + registry lookup
    // -------------------------------------------------------------------------

    private void deadLetter(String rawPayload, ReasonCode reason, String messageId, String interactionId) {
        failedCounters.computeIfAbsent(reason.name(), r -> meterRegistry.counter("kafka.processor.messages.failed", "reason", r)).increment();
        deadLetterService.handle(rawPayload, reason, messageId, interactionId);
    }

    private Counter counter(ConcurrentHashMap<String, Counter> cache, String eventType, String name) {
        return cache.computeIfAbsent(eventType, et -> meterRegistry.counter(name, "eventType", et));
    }

    private Timer timer(ConcurrentHashMap<String, Timer> cache, String eventType, String name) {
        return cache.computeIfAbsent(eventType, et -> meterRegistry.timer(name, "eventType", et));
    }

    private static boolean isValidUuid(String value) {
        if (value == null || value.length() != 36) return false;
        try {
            java.util.UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Nested types
    // -------------------------------------------------------------------------

    // Bundles per-message data so it can be passed as a unit rather than as individual parameters.
    private record MessageContext(
            String rawPayload, KafkaMessage message,
            String messageId, String interactionId, String eventType) {}
}
