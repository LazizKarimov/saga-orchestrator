package com.example.sagaorchestrator.kafka.consumer;

import com.example.sagaorchestrator.event.PaymentCompletedEvent;
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
public class PaymentEventConsumer {

    private final SagaOrchestratorService orchestratorService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "payment-events",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentCompleted(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[Saga] Получен PaymentCompletedEvent: {}", record.value());
        try {
            PaymentCompletedEvent event = objectMapper.readValue(record.value(), PaymentCompletedEvent.class);
            orchestratorService.onPaymentCompleted(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Ошибка обработки PaymentCompletedEvent: {}", record.value(), e);
        }
    }
}