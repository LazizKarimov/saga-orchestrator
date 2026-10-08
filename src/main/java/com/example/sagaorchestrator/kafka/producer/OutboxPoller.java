package com.example.sagaorchestrator.kafka.producer;

import com.example.sagaorchestrator.entity.OutboxEvent;
import com.example.sagaorchestrator.repository.OutboxEventRepository;
import com.example.sagaorchestrator.service.OutboxDlqService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxDlqService outboxDlqService;

    @Value("${outbox.poll.batch-size:100}")
    private int batchSize;

    @Value("${outbox.poll.max-attempts:5}")
    private int maxAttempts;

    @Value("${outbox.poll.base-backoff-ms:2000}")
    private long baseBackoffMs;

    @Scheduled(fixedDelayString = "${outbox.poll.interval-ms:2000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxRepository.findReadyToSend(maxAttempts, batchSize);
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxEvent event : batch) {
            try {
                Object payload = objectMapper.readValue(event.getPayload(), Object.class);
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), payload).get();

                event.setProcessedAt(LocalDateTime.now());
                event.setLastError(null);
                log.info("Outbox → Kafka: type={}, aggregateId={}, topic={}",
                        event.getEventType(), event.getAggregateId(), event.getTopic());
            } catch (Exception e) {
                int newAttempts = event.getAttempts() + 1;
                event.setAttempts(newAttempts);
                event.setLastError(truncate(e.getMessage(), 2000));

                if (newAttempts >= maxAttempts) {
                    log.error("Outbox {} окончательно не отправлен после {} попыток: {}",
                            event.getId(), newAttempts, e.getMessage());
                    // next_attempt_at не ставим — условие attempts < maxAttempts
                    // отфильтрует его на следующих циклах
                } else {
                    long backoffMs = computeBackoffMs(newAttempts);
                    event.setNextAttemptAt(
                            LocalDateTime.now().plusNanos(backoffMs * 1_000_000));
                    log.warn("Outbox {} попытка {}/{} не удалась: {} (retry через {} ms)",
                            event.getId(), newAttempts, maxAttempts,
                            e.getMessage(), backoffMs);
                }
            }
        }
    }

    @Scheduled(fixedDelayString = "${outbox.dlq.move-interval-ms:300000}")
    public void moveExhausted() {
        outboxDlqService.moveExhaustedToDlq();
    }

    /**
     * Экспоненциальный backoff: base * 2^(attempts-1).
     * attempt 1 → base (2000 ms)
     * attempt 2 → base * 2 (4000 ms)
     * attempt 3 → base * 4 (8000 ms)
     * attempt 4 → base * 8 (16000 ms)
     */
    private long computeBackoffMs(int attempts) {
        return baseBackoffMs * (1L << (attempts - 1));
    }

    private String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}