package com.example.kafkametrics.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed binding for all {@code app.*} configuration properties.
 *
 * <p>Registered via {@code @EnableConfigurationProperties(AppProperties.class)} on the
 * main application class. Inject this bean wherever app settings are needed in preference
 * to scattered {@code @Value} annotations.
 */
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private Processing processing = new Processing();

    public Processing getProcessing() { return processing; }
    public void setProcessing(Processing processing) { this.processing = processing; }

    public static class Processing {

        private long processorTimeoutMs = 10000;
        private long lookupTimeoutMs = 2000;
        private long statusLogIntervalMs = 10000;
        private int workerThreads = 200;

        public long getProcessorTimeoutMs() { return processorTimeoutMs; }
        public void setProcessorTimeoutMs(long processorTimeoutMs) { this.processorTimeoutMs = processorTimeoutMs; }

        public long getLookupTimeoutMs() { return lookupTimeoutMs; }
        public void setLookupTimeoutMs(long lookupTimeoutMs) { this.lookupTimeoutMs = lookupTimeoutMs; }

        public long getStatusLogIntervalMs() { return statusLogIntervalMs; }
        public void setStatusLogIntervalMs(long statusLogIntervalMs) { this.statusLogIntervalMs = statusLogIntervalMs; }

        public int getWorkerThreads() { return workerThreads; }
        public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    }
}
