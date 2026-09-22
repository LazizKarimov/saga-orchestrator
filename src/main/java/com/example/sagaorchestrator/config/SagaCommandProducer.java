package com.example.sagaorchestrator.config;

import com.example.sagaorchestrator.dto.ProcessPaymentCommand;
import com.example.sagaorchestrator.dto.ReserveInventoryCommand;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaCommandProducer {

    private static final String PAYMENT_COMMANDS_TOPIC = "payment-commands";
    private static final String RESERVE_INVENTORY_TOPIC = "reserve-inventory-command";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendProcessPaymentCommand(ProcessPaymentCommand command) {
        kafkaTemplate.send(PAYMENT_COMMANDS_TOPIC, command.sagaId().toString(), command)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("ProcessPaymentCommand отправлен: sagaId={}, orderId={}",
                                command.sagaId(), command.orderId());
                    } else {
                        log.error("Ошибка отправки ProcessPaymentCommand: sagaId={}",
                                command.sagaId(), ex);
                    }
                });
    }

    public void sendReserveInventoryCommand(ReserveInventoryCommand command) {
        kafkaTemplate.send(RESERVE_INVENTORY_TOPIC, command.getSagaId(), command)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("ReserveInventoryCommand отправлен: sagaId={}",
                                command.getSagaId());
                    } else {
                        log.error("Ошибка отправки ReserveInventoryCommand: sagaId={}",
                                command.getSagaId(), ex);
                    }
                });
    }
}