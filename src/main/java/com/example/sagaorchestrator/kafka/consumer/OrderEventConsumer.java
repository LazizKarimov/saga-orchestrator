package com.example.sagaorchestrator.kafka.consumer;

import com.example.sagaorchestrator.event.OrderCreatedEvent;
import com.example.sagaorchestrator.service.SagaOrchestratorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final SagaOrchestratorService sagaService;

    @KafkaListener(
            topics = "order-events",
            groupId = "saga-orchestrator-group",
            properties = {
                    "spring.json.value.default.type=com.example.sagaorchestrator.event.OrderCreatedEvent"
            }
    )
    public void consumeOrderCreated(OrderCreatedEvent event, Acknowledgment ack) {
        log.info(" [Saga] Получен OrderCreatedEvent: orderId={}", event.getId());

        try {
            sagaService.startSaga(event);
            ack.acknowledge();
            log.info(" Событие подтверждено");
        } catch (Exception e) {
            log.error(" Ошибка запуска саги", e);
            // Не подтверждаем — Kafka перечитает
        }
    }
}