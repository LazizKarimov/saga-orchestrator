package com.example.sagaorchestrator.kafka.producer;

import com.example.sagaorchestrator.entity.OutboxEvent;
import com.example.sagaorchestrator.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class OutboxEventPublisher {

    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * Сохраняет событие в outbox. Вызывать внутри транзакции,
     * в которой сохраняется SagaInstance — тогда либо оба коммита,
     * либо оба отката.
     */
    public void enqueue(String aggregateId, String eventType, String topic, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            OutboxEvent event = OutboxEvent.builder()
                    .id(UUID.randomUUID())
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .topic(topic)
                    .payload(json)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxRepository.save(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "Не удалось сериализовать payload для outbox: " + eventType, e);
        }
    }
}