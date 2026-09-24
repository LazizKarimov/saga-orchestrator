package com.example.sagaorchestrator.kafka.producer;

import com.example.sagaorchestrator.entity.OutboxEvent;
import com.example.sagaorchestrator.repository.OutboxEventRepository;
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

    @Value("${outbox.poll.batch-size:100}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${outbox.poll.interval-ms:2000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxRepository.findUnprocessedBatch(batchSize);
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxEvent event : batch) {
            try {
                Object payload = objectMapper.readValue(event.getPayload(), Object.class);
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), payload).get();

                event.setProcessedAt(LocalDateTime.now());
                log.info("Outbox → Kafka: type={}, aggregateId={}, topic={}",
                        event.getEventType(), event.getAggregateId(), event.getTopic());
            } catch (Exception e) {
                log.error("Не удалось отправить outbox {}: {}",
                        event.getId(), e.getMessage());
                // processedAt остаётся null → повтор на следующем цикле
            }
        }
    }
}