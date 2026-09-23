package com.example.sagaorchestrator.kafka.consumer;

import com.example.sagaorchestrator.dto.InventoryReservationFailedEvent;
import com.example.sagaorchestrator.service.SagaOrchestratorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryFailedEventConsumer {

    private final SagaOrchestratorService orchestratorService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "inventory-reservation-failed-events",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeInventoryReservationFailed(ConsumerRecord<String, String> record,
                                                  Acknowledgment ack) {
        log.info("[Saga] Получен InventoryReservationFailedEvent: {}", record.value());
        try {
            InventoryReservationFailedEvent event =
                    objectMapper.readValue(record.value(), InventoryReservationFailedEvent.class);
            orchestratorService.onInventoryReservationFailed(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Ошибка обработки InventoryReservationFailedEvent: {}", record.value(), e);
            // не ack — Kafka перечитает
        }
    }
}