package com.example.sagaorchestrator.kafka.consumer;

import com.example.sagaorchestrator.event.OrderCancelledEvent;
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
public class OrderCancelledEventConsumer {

    private final SagaOrchestratorService orchestratorService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order-cancelled-events",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCancelled(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[Saga] Получен OrderCancelledEvent: {}", record.value());
        try {
            OrderCancelledEvent event =
                    objectMapper.readValue(record.value(), OrderCancelledEvent.class);
            orchestratorService.onOrderCancelled(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Ошибка обработки OrderCancelledEvent: {}", record.value(), e);
            // не ack — Kafka перечитает
        }
    }
}