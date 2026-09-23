package com.example.sagaorchestrator.kafka.consumer;

import com.example.sagaorchestrator.dto.PaymentRefundedEvent;
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
public class PaymentRefundedEventConsumer {

    private final SagaOrchestratorService orchestratorService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "payment-refunded-events",
            groupId = "${spring.kafka.consumer.group-id:saga-orchestrator-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentRefunded(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[Saga] Получен PaymentRefundedEvent: {}", record.value());
        try {
            PaymentRefundedEvent event =
                    objectMapper.readValue(record.value(), PaymentRefundedEvent.class);
            orchestratorService.onPaymentRefunded(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Ошибка обработки PaymentRefundedEvent: {}", record.value(), e);
            // не ack — Kafka перечитает
        }
    }
}