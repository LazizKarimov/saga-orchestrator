package com.example.sagaorchestrator.service;

import com.example.sagaorchestrator.entity.OutboxDlqEvent;
import com.example.sagaorchestrator.entity.OutboxEvent;
import com.example.sagaorchestrator.repository.OutboxDlqRepository;
import com.example.sagaorchestrator.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxDlqService {

    private final OutboxEventRepository outboxRepository;
    private final OutboxDlqRepository dlqRepository;

    @Value("${outbox.poll.max-attempts:5}")
    private int maxAttempts;

    /**
     * Переносит «убитые» записи (attempts >= max, processed_at IS NULL)
     * из outbox_events в outbox_dlq.
     */
    @Transactional
    public int moveExhaustedToDlq() {
        List<OutboxEvent> exhausted = outboxRepository.findExhausted(maxAttempts);
        if (exhausted.isEmpty()) {
            return 0;
        }

        for (OutboxEvent e : exhausted) {
            dlqRepository.save(OutboxDlqEvent.builder()
                    .id(UUID.randomUUID())
                    .originalId(e.getId())
                    .aggregateId(e.getAggregateId())
                    .eventType(e.getEventType())
                    .topic(e.getTopic())
                    .payload(e.getPayload())
                    .createdAt(e.getCreatedAt())
                    .failedAt(LocalDateTime.now())
                    .attempts(e.getAttempts())
                    .lastError(e.getLastError())
                    .build());
            outboxRepository.delete(e);
        }

        log.warn("Перенесено в DLQ: {} записей", exhausted.size());
        return exhausted.size();
    }
}