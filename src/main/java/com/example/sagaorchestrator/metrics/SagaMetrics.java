package com.example.sagaorchestrator.metrics;

import com.example.sagaorchestrator.repository.OutboxDlqRepository;
import com.example.sagaorchestrator.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SagaMetrics {

    private final Counter outboxSentTotal;
    private final Counter outboxFailedTotal;
    private final Counter sagaStartedTotal;
    private final Counter sagaCompletedTotal;
    private final Counter sagaCompensatedTotal;

    public SagaMetrics(MeterRegistry registry,
                       OutboxEventRepository outboxRepository,
                       OutboxDlqRepository dlqRepository) {

        this.outboxSentTotal = Counter.builder("outbox_sent_total")
                .description("Total outbox messages successfully sent to Kafka")
                .register(registry);

        this.outboxFailedTotal = Counter.builder("outbox_failed_total")
                .description("Total outbox send failures")
                .register(registry);

        this.sagaStartedTotal = Counter.builder("saga_started_total")
                .description("Total sagas started")
                .register(registry);

        this.sagaCompletedTotal = Counter.builder("saga_completed_total")
                .description("Total sagas completed successfully")
                .register(registry);

        this.sagaCompensatedTotal = Counter.builder("saga_compensated_total")
                .description("Total sagas compensated")
                .register(registry);

        // Gauge — считает значение на каждый scrape Prometheus
        Gauge.builder("outbox_pending", outboxRepository,
                        repo -> repo.countByProcessedAtIsNull())
                .description("Number of pending outbox records")
                .register(registry);

        Gauge.builder("outbox_dlq_size", dlqRepository,
                        repo -> repo.count())
                .description("Number of records in DLQ")
                .register(registry);
    }

    public void recordOutboxSent() {
        outboxSentTotal.increment();
    }

    public void recordOutboxFailed() {
        outboxFailedTotal.increment();
    }

    public void recordSagaStarted() {
        sagaStartedTotal.increment();
    }

    public void recordSagaCompleted() {
        sagaCompletedTotal.increment();
    }

    public void recordSagaCompensated() {
        sagaCompensatedTotal.increment();
    }
}