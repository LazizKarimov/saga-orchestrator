package com.example.sagaorchestrator.kafka.consumer;

import com.example.sagaorchestrator.dto.InventoryReservedEvent;
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
public class InventoryEventConsumer {

    private final SagaOrchestratorService orchestratorService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "inventory-reserved-event",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeInventoryReserved(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[Saga] Получен InventoryReservedEvent: {}", record.value());
        try {
            InventoryReservedEvent event = objectMapper.readValue(record.value(), InventoryReservedEvent.class);
            orchestratorService.onInventoryReserved(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Ошибка обработки InventoryReservedEvent: {}", record.value(), e);
        }
    }
}