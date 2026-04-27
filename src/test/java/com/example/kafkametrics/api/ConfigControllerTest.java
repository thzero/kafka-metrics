package com.example.kafkametrics.api;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.kafkametrics.config.AppProperties;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ConfigController.class)
@TestPropertySource(properties = {
        "kafka.bootstrap-servers=localhost:9092",
        "kafka.consumer.group-id=test-group",
        "kafka.consumer.concurrency=1",
        "kafka.topic.input=input-topic",
        "kafka.topic.output=output-topic"})
class ConfigControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AppProperties appProperties;

    @Test
    void getConfig_returnsAllCurrentSettings() throws Exception {
        AppProperties.Processing processing = new AppProperties.Processing();
        processing.setWorkerThreads(200);
        when(appProperties.getProcessing()).thenReturn(processing);

        mockMvc.perform(get("/api/config"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.kafka.bootstrapServers").value("localhost:9092"))
                .andExpect(jsonPath("$.kafka.consumerGroupId").value("test-group"))
                .andExpect(jsonPath("$.kafka.consumerConcurrency").value(1))
                .andExpect(jsonPath("$.kafka.inputTopic").value("input-topic"))
                .andExpect(jsonPath("$.kafka.outputTopic").value("output-topic"))
                .andExpect(jsonPath("$.app.processorDelayMs").value(20000))
                .andExpect(jsonPath("$.app.workerThreads").value(200))
                .andExpect(jsonPath("$.app.siphonEnabledEvaluators[0]").value("bde"));
    }
}
