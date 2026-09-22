package com.example.sagaorchestrator.kafka.producer;

import com.example.sagaorchestrator.event.SagaCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaEventProducer {

    private static final String SAGA_EVENTS_TOPIC = "saga-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendSagaCompletedEvent(SagaCompletedEvent event) {
        kafkaTemplate.send(SAGA_EVENTS_TOPIC, event.sagaId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("SagaCompletedEvent отправлен: sagaId={}", event.sagaId());
                    } else {
                        log.error("Ошибка отправки SagaCompletedEvent: sagaId={}",
                                event.sagaId(), ex);
                    }
                });
    }
}