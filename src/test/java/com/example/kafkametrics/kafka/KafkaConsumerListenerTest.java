package com.example.kafkametrics.kafka;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.example.kafkametrics.control.IControlService;
import com.example.kafkametrics.deadletter.IDeadLetterService;
import com.example.kafkametrics.deadletter.ReasonCode;
import com.example.kafkametrics.processor.IEventProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerListenerTest {

    @Mock private IControlService controlService;
    @Mock private IEventProcessor processor;
    @Mock private IDeadLetterService deadLetterService;
    @Mock private Acknowledgment acknowledgment;

    private KafkaConsumerListener listener;
    private ObjectMapper objectMapper;

    // A real single-thread scheduler is used so submitted tasks actually execute.
    private ScheduledExecutorService scheduler;

    private static final String MSG_ID_1 = "00000000-0000-0000-0000-000000000001";
    private static final String IID_1 = "iid-1";

    private static final String VALID_PAYLOAD =
            "{\"header\":{\"messageId\":\"00000000-0000-0000-0000-000000000001\",\"interactionId\":\"iid-1\",\"eventType\":\"TEST\"},\"payload\":{}}";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        var created = new KafkaConsumerListener(
                objectMapper, controlService,
                processor, deadLetterService, scheduler,
                new SimpleMeterRegistry());
        listener = created;
    }

    private ConsumerRecord<String, String> record(String payload) {
        return new ConsumerRecord<>("input-topic", 0, 0L, null, payload);
    }

    // Waits for the scheduler to finish all currently queued tasks
    private void awaitScheduler() throws InterruptedException {
        scheduler.schedule(() -> {}, 0, TimeUnit.MILLISECONDS);
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void happyPath_writesControlRecords_publishesAndAcks() throws InterruptedException {
        // messageProcessorService.process() is void — default mock behaviour (do nothing) is correct.
        // The actual publish is done inside the EventProcessor, not in the listener.

        listener.listen(record(VALID_PAYLOAD), acknowledgment);
        awaitScheduler();

        verify(controlService).recordReceived(MSG_ID_1, IID_1);
        verify(processor).process(any(), any());
        verify(controlService).recordPublished(MSG_ID_1, IID_1);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterService);
    }

    @Test
    void invalidMessageId_routesToDeadLetter_noAck() {
        String badPayload = "{\"header\":{\"messageId\":\"not-a-uuid\",\"interactionId\":\"iid-1\",\"eventType\":\"TEST\"},\"payload\":{}}";
        listener.listen(record(badPayload), acknowledgment);

        verify(deadLetterService).handle(eq(badPayload), eq(ReasonCode.INVALID_MESSAGE_ID), eq("not-a-uuid"), eq(IID_1));
        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(controlService, processor);
    }

    @Test
    void nullMessageId_routesToDeadLetter_noAck() {
        String noMessageIdPayload = "{\"header\":{\"interactionId\":\"iid-1\",\"eventType\":\"TEST\"},\"payload\":{}}";
        listener.listen(record(noMessageIdPayload), acknowledgment);

        verify(deadLetterService).handle(eq(noMessageIdPayload), eq(ReasonCode.INVALID_MESSAGE_ID), isNull(), eq(IID_1));
        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(controlService, processor);
    }

    @Test
    void deserializationFailure_routesToDeadLetter_noAck() throws InterruptedException {
        listener.listen(record("not-valid-json"), acknowledgment);
        awaitScheduler();

        verify(deadLetterService).handle(eq("not-valid-json"), eq(ReasonCode.DESERIALIZATION_ERROR), isNull(), isNull());
        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(controlService, processor);
    }

    @Test
    void duplicateMessage_inFlight_routesToDeadLetter_acks() {
        // Use a mock scheduler so the first message's worker task is captured but never executed,
        // keeping its messageId in the in-flight set when the second message arrives.
        ScheduledExecutorService nonExecutingScheduler = mock(ScheduledExecutorService.class);
        var l = new KafkaConsumerListener(
                objectMapper, controlService, processor,
                deadLetterService, nonExecutingScheduler,
                new SimpleMeterRegistry());

        Acknowledgment ack2 = mock(Acknowledgment.class);

        // First arrival — accepted, scheduled (task never executes — mock scheduler)
        l.listen(record(VALID_PAYLOAD), acknowledgment);
        // Second arrival with same messageId — hits in-flight duplicate gate
        l.listen(record(VALID_PAYLOAD), ack2);

        verify(deadLetterService).handle(eq(VALID_PAYLOAD), eq(ReasonCode.DUPLICATE), eq(MSG_ID_1), eq(IID_1));
        verify(ack2).acknowledge();
        verify(acknowledgment, never()).acknowledge();
        verifyNoInteractions(processor, controlService);
    }

    @Test
    void processingFailure_routesToDeadLetter_noAck() throws InterruptedException {
        doThrow(new ProcessingException("boom")).when(processor).process(any(), any());

        listener.listen(record(VALID_PAYLOAD), acknowledgment);
        awaitScheduler();

        verify(deadLetterService).handle(eq(VALID_PAYLOAD), eq(ReasonCode.PROCESSING_ERROR), eq(MSG_ID_1), eq(IID_1));
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void publishFailure_routesToDeadLetter_noAck() throws InterruptedException {
        doThrow(new KafkaPublishException("publish failed", new RuntimeException()))
                .when(processor).process(any(), any());

        listener.listen(record(VALID_PAYLOAD), acknowledgment);
        awaitScheduler();

        verify(deadLetterService).handle(eq(VALID_PAYLOAD), eq(ReasonCode.PUBLISH_ERROR), eq(MSG_ID_1), eq(IID_1));
        verify(acknowledgment, never()).acknowledge();
        verify(controlService, never()).recordPublished(any(), any());
    }
}
