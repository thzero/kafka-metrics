package com.example.kafkametrics.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.Instant;
import java.util.List;

@DataJpaTest
@Import(ControlServiceImpl.class)
class ControlServiceImplTest {

    @Autowired
    private IControlService controlService;

    @Autowired
    private IReceivedRecordRepository receivedRepository;

    @Autowired
    private IPublishedRecordRepository publishedRepository;

    private static final String MSG_ID_1 = "00000000-0000-0000-0000-000000000001";
    private static final String MSG_ID_2 = "00000000-0000-0000-0000-000000000002";
    private static final String MSG_ID_3 = "00000000-0000-0000-0000-000000000003";

    @Test
    void recordReceived_persistsRecordWithCorrectFields() {
        controlService.recordReceived(MSG_ID_1, "interaction-1");

        List<ReceivedRecord> records = receivedRepository.findAll();
        assertThat(records).hasSize(1);
        ReceivedRecord record = records.get(0);
        assertThat(record.getMessageId()).isEqualTo(MSG_ID_1);
        assertThat(record.getInteractionId()).isEqualTo("interaction-1");
        assertThat(record.getReceivedAt()).isNotNull();
    }

    @Test
    void recordReceived_duplicateMessageId_throwsDataIntegrityViolation() {
        String msgId = "00000000-0000-0000-0000-0000000000dd";
        controlService.recordReceived(msgId, "interaction-1");

        assertThatThrownBy(() -> controlService.recordReceived(msgId, "interaction-2"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void recordPublished_persistsRecordWithCorrectFields() {
        controlService.recordPublished(MSG_ID_2, "interaction-2");

        List<PublishedRecord> records = publishedRepository.findAll();
        assertThat(records).hasSize(1);
        PublishedRecord record = records.get(0);
        assertThat(record.getMessageId()).isEqualTo(MSG_ID_2);
        assertThat(record.getInteractionId()).isEqualTo("interaction-2");
        assertThat(record.getPublishedAt()).isNotNull();
    }

    @Test
    void findByReceivedAtGreaterThanEqual_returnsRecordsInRange() {
        Instant before = Instant.now().minusSeconds(60);
        controlService.recordReceived(MSG_ID_3, "interaction-3");

        List<ReceivedRecord> results = receivedRepository.findByReceivedAtGreaterThanEqual(before);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getMessageId()).isEqualTo(MSG_ID_3);
    }
}
