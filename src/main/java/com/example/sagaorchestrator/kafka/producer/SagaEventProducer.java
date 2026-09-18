package com.example.sagaorchestrator.kafka.producer;

import com.example.sagaorchestrator.dto.ReserveInventoryCommand;
import com.example.sagaorchestrator.event.SagaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "saga-events";

    public void sendSagaEvent(SagaEvent event) {
        kafkaTemplate.send(TOPIC, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info(" SagaEvent отправлен: step={}, status={}",
                                event.getStep(), event.getStatus());
                    } else {
                        log.error(" Ошибка отправки SagaEvent", ex);
                    }
                });
    }

    public void sendReserveInventoryCommand(ReserveInventoryCommand command) {
        kafkaTemplate.send("reserve-inventory-command", command.getSagaId(), command);
    }
}