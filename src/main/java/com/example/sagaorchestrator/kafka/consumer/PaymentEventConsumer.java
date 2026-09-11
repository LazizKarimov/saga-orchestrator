package com.example.sagaorchestrator.kafka.consumer;

import com.example.sagaorchestrator.event.PaymentCompletedEvent;
import com.example.sagaorchestrator.service.SagaOrchestratorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventConsumer {

    private final SagaOrchestratorService sagaService;

    @KafkaListener(
            topics = "payment-events",
            groupId = "saga-orchestrator-group",
            properties = {
                    "spring.json.value.default.type=com.example.sagaorchestrator.event.PaymentCompletedEvent"}
    )
    public void consumePaymentCompleted(PaymentCompletedEvent event, Acknowledgment ack) {
        log.info(" [Saga] Получен PaymentCompletedEvent: orderId={}", event.getOrderId());

        try {
            sagaService.onPaymentCompleted(event);
            ack.acknowledge();
            log.info(" Событие подтверждено");
        } catch (Exception e) {
            log.error(" Ошибка обработки PaymentCompletedEvent", e);
        }
    }
}