package com.example.kafkametrics.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@Component("processorThreadPool")
public class ProcessorHealthIndicator implements HealthIndicator {

    private final ScheduledExecutorService processingScheduler;
    private final int workerThreads;

    public ProcessorHealthIndicator(ScheduledExecutorService processingScheduler,
                                    @Value("${app.processing.worker-threads:200}") int workerThreads) {
        this.processingScheduler = processingScheduler;
        this.workerThreads = workerThreads;
    }

    @Override
    public Health health() {
        if (processingScheduler.isShutdown() || processingScheduler.isTerminated()) {
            return Health.down().withDetail("reason", "processingScheduler is shut down").build();
        }
        if (!(processingScheduler instanceof ThreadPoolExecutor tpe)) {
            return Health.unknown().withDetail("reason", "not a ThreadPoolExecutor").build();
        }
        int active = tpe.getActiveCount();
        int queueSize = tpe.getQueue().size();
        long completed = tpe.getCompletedTaskCount();
        double util = workerThreads > 0 ? (double) active / workerThreads : 0.0;

        Health.Builder builder = util >= 1.0
                ? Health.outOfService().withDetail("reason", "thread pool saturated")
                : Health.up();

        return builder
                .withDetail("configuredThreads", workerThreads)
                .withDetail("activeThreads",     active)
                .withDetail("queuedTasks",       queueSize)
                .withDetail("completedTasks",    completed)
                .withDetail("utilizationPct",    Math.round(util * 100) + "%")
                .build();
    }
}
