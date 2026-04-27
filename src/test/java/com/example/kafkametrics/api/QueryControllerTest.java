package com.example.kafkametrics.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.kafkametrics.control.IPublishedRecordRepository;
import com.example.kafkametrics.control.ReceivedRecord;
import com.example.kafkametrics.control.IReceivedRecordRepository;
import com.example.kafkametrics.deadletter.DeadLetterRecord;
import com.example.kafkametrics.deadletter.IDeadLetterRepository;
import com.example.kafkametrics.deadletter.ReasonCode;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@WebMvcTest(QueryController.class)
class QueryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private IReceivedRecordRepository receivedRecordRepository;

    @MockitoBean
    private IPublishedRecordRepository publishedRecordRepository;

    @MockitoBean
    private IDeadLetterRepository deadLetterRepository;

    private static final String MSG_ID_1 = "00000000-0000-0000-0000-000000000001";
    private static final String MSG_ID_2 = "00000000-0000-0000-0000-000000000002";

    @Test
    void getInbound_noParams_defaultsToLast12Hours() throws Exception {
        when(receivedRecordRepository.findByReceivedAtGreaterThanEqual(any(Instant.class)))
                .thenReturn(List.of());

        mockMvc.perform(get("/api/control/inbound"))
                .andExpect(status().isOk());

        verify(receivedRecordRepository).findByReceivedAtGreaterThanEqual(any(Instant.class));
    }

    @Test
    void getInbound_withBothParams_appliesRange() throws Exception {
        ReceivedRecord record = new ReceivedRecord();
        record.setMessageId(MSG_ID_1);
        record.setInteractionId("iid-1");
        record.setReceivedAt(Instant.now());

        when(receivedRecordRepository.findByReceivedAtBetween(any(Instant.class), any(Instant.class)))
                .thenReturn(List.of(record));

        String start = Instant.now().minus(6, ChronoUnit.HOURS).toString();
        String end = Instant.now().toString();

        mockMvc.perform(get("/api/control/inbound")
                        .param("startTimestamp", start)
                        .param("endTimestamp", end))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].messageId").value(MSG_ID_1));
    }

    @Test
    void getOutbound_noParams_defaultsToLast12Hours() throws Exception {
        when(publishedRecordRepository.findByPublishedAtGreaterThanEqual(any(Instant.class)))
                .thenReturn(List.of());

        mockMvc.perform(get("/api/control/outbound"))
                .andExpect(status().isOk());

        verify(publishedRecordRepository).findByPublishedAtGreaterThanEqual(any(Instant.class));
    }

    @Test
    void getDeadLetter_noParams_defaultsToLast12Hours() throws Exception {
        when(deadLetterRepository.findByFailedAtGreaterThanEqual(any(Instant.class)))
                .thenReturn(List.of());

        mockMvc.perform(get("/api/deadletter"))
                .andExpect(status().isOk());

        verify(deadLetterRepository).findByFailedAtGreaterThanEqual(any(Instant.class));
    }

    @Test
    void getDeadLetter_withBothParams_appliesRange() throws Exception {
        DeadLetterRecord record = new DeadLetterRecord();
        record.setMessageId(MSG_ID_2);
        record.setInteractionId("iid-2");
        record.setReasonCode(ReasonCode.PUBLISH_ERROR);
        record.setRawPayload("{}");
        record.setFailedAt(Instant.now());

        when(deadLetterRepository.findByFailedAtBetween(any(Instant.class), any(Instant.class)))
                .thenReturn(List.of(record));

        String start = Instant.now().minus(6, ChronoUnit.HOURS).toString();
        String end = Instant.now().toString();

        mockMvc.perform(get("/api/deadletter")
                        .param("startTimestamp", start)
                        .param("endTimestamp", end))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].messageId").value(MSG_ID_2))
                .andExpect(jsonPath("$[0].reasonCode").value("PUBLISH_ERROR"));
    }
}
