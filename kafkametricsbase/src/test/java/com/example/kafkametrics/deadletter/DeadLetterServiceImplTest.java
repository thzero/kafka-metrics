package com.example.kafkametrics.deadletter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.List;

@DataJpaTest
@Import(DeadLetterServiceImpl.class)
class DeadLetterServiceImplTest {

    @Autowired
    private IDeadLetterService deadLetterService;

    @Autowired
    private IDeadLetterRepository repository;

    private static final String MSG_ID_1 = "00000000-0000-0000-0000-000000000001";
    private static final String MSG_ID_2 = "00000000-0000-0000-0000-000000000002";

    @Test
    void handle_persistsRecordWithCorrectFields() {
        deadLetterService.handle("{\"event\":{}}", ReasonCode.PROCESSING_ERROR, MSG_ID_1, "interaction-1");

        List<DeadLetterRecord> records = repository.findAll();
        assertThat(records).hasSize(1);
        DeadLetterRecord record = records.get(0);
        assertThat(record.getRawPayload()).isEqualTo("{\"event\":{}}");
        assertThat(record.getReasonCode()).isEqualTo(ReasonCode.PROCESSING_ERROR);
        assertThat(record.getMessageId()).isEqualTo(MSG_ID_1);
        assertThat(record.getInteractionId()).isEqualTo("interaction-1");
        assertThat(record.getFailedAt()).isNotNull();
    }

    @Test
    void handle_allowsNullMessageIdAndInteractionId() {
        deadLetterService.handle("bad-json", ReasonCode.DESERIALIZATION_ERROR, null, null);

        List<DeadLetterRecord> records = repository.findAll();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getMessageId()).isNull();
        assertThat(records.get(0).getInteractionId()).isNull();
    }

    @Test
    void findByFailedAtGreaterThanEqual_returnsRecordsInRange() {
        Instant before = Instant.now().minusSeconds(60);
        deadLetterService.handle("{}", ReasonCode.PUBLISH_ERROR, MSG_ID_2, "interaction-2");

        List<DeadLetterRecord> results = repository.findByFailedAtGreaterThanEqual(before);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getReasonCode()).isEqualTo(ReasonCode.PUBLISH_ERROR);
    }
}
