package com.example.sagaorchestrator.service;

import com.example.sagaorchestrator.dto.InventoryReservedEvent;
import com.example.sagaorchestrator.dto.ReserveInventoryCommand;
import com.example.sagaorchestrator.entity.SagaInstance;
import com.example.sagaorchestrator.entity.SagaStatus;
import com.example.sagaorchestrator.entity.SagaStep;
import com.example.sagaorchestrator.event.OrderCreatedEvent;
import com.example.sagaorchestrator.event.PaymentCompletedEvent;
import com.example.sagaorchestrator.event.SagaEvent;
import com.example.sagaorchestrator.kafka.producer.SagaEventProducer;
import com.example.sagaorchestrator.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;


@Service
@RequiredArgsConstructor
@Slf4j
public class SagaOrchestratorService {

    private final SagaInstanceRepository sagaRepository;
    private final SagaEventProducer sagaEventProducer;
    private final ObjectMapper objectMapper;

    /**
     * Шаг 1: Получен OrderCreatedEvent → запускаем сагу
     */
    @Transactional
    public void startSaga(OrderCreatedEvent event) {
        log.info(" Запуск саги для заказа: {}", event.getId());

        // Идемпотентность
        if (sagaRepository.existsByOrderId(event.getId())) {
            log.warn(" Сага для заказа {} уже существует", event.getId());
            return;
        }

        SagaInstance saga = SagaInstance.builder()
                .orderId(event.getId())
                .customerId(event.getCustomerId())
                .status(SagaStatus.STARTED)
                .currentStep(SagaStep.ORDER_CREATED)
                .payload(toJson(event))
                .build();

        saga = sagaRepository.save(saga); // мб тут не нужно сохр т к entityManager сам сохранит
        log.info(" Сага создана: id={}", saga.getId());

        // Переходим к шагу PAYMENT_PROCESSING
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga.setCurrentStep(SagaStep.PAYMENT_PROCESSING);
        sagaRepository.save(saga);

        // Отправляем команду на оплату
        sagaEventProducer.sendSagaEvent(SagaEvent.builder()
                .sagaId(saga.getId())
                .orderId(saga.getOrderId())
                .customerId(saga.getCustomerId())
                .amount(event.getAmount())
                .step(SagaStep.PAYMENT_PROCESSING.name())
                .status("COMMAND")
                .eventType("PROCESS_PAYMENT")
                .timestamp(System.currentTimeMillis())
                .build());
    }

    @Transactional
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Получен PaymentCompletedEvent: orderId={}", event.getOrderId());

        Optional<SagaInstance> maybeSaga = sagaRepository.findByOrderId(event.getOrderId());
        if (maybeSaga.isEmpty()) {
            log.warn("Сага для заказа {} не найдена, событие игнорируется", event.getOrderId());
            return;
        }
        SagaInstance saga = maybeSaga.get();

        if (saga.getCurrentStep() != SagaStep.PAYMENT_PROCESSING) {
            log.warn("Сага на шаге {}, ожидался PAYMENT_PROCESSING", saga.getCurrentStep());
            return;
        }

        saga.setCurrentStep(SagaStep.INVENTORY_PROCESSING);
        saga.setStatus(SagaStatus.IN_PROGRESS);
        sagaRepository.save(saga);

        OrderCreatedEvent orderEvent = parseOrderCreated(saga);

        ReserveInventoryCommand command = ReserveInventoryCommand.builder()
                .sagaId(String.valueOf(saga.getId()))
                .orderId(String.valueOf(saga.getOrderId()))
                .items(orderEvent.getItems().stream()
                        .map(i -> ReserveInventoryCommand.Item.builder()
                                .productId(i.getProductId())
                                .quantity(i.getQuantity())
                                .build())
                        .toList())
                .build();

        sagaEventProducer.sendReserveInventoryCommand(command);
        log.info("Отправлена команда RESERVE_INVENTORY для саги {}", saga.getId());
    }

    /**
     * Завершение саги
     */
    @Transactional
    public void completeSaga(SagaInstance saga) {
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setCurrentStep(SagaStep.COMPLETED);
        saga.setCompletedAt(LocalDateTime.now());
        sagaRepository.save(saga);

        log.info(" Сага завершена: id={}, orderId={}", saga.getId(), saga.getOrderId());

        // Отправляем событие о завершении саги
        sagaEventProducer.sendSagaEvent(SagaEvent.builder()
                .sagaId(saga.getId())
                .orderId(saga.getOrderId())
                .amount(null)
                .step(SagaStep.COMPLETED.name())
                .status("COMPLETED")
                .eventType("SAGA_COMPLETED")
                .timestamp(System.currentTimeMillis())
                .build());
    }

    /**
     * Компенсация — откат саги при ошибке
     */
    @Transactional
    public void compensateSaga(SagaInstance saga, String errorMessage) {
        log.warn(" Запуск компенсации саги: id={}, error={}", saga.getId(), errorMessage);

        saga.setStatus(SagaStatus.COMPENSATING);
        saga.setErrorMessage(errorMessage);
        sagaRepository.save(saga);

        // Логика компенсаций (зависит от текущего шага)
        switch (saga.getCurrentStep()) {
            case PAYMENT_PROCESSING -> {
                // Платёж не прошёл — просто отменяем заказ
                log.info(" Компенсация: отмена заказа {}", saga.getOrderId());
            }
            case PAYMENT_COMPLETED -> {
                // Платёж прошёл, но что-то упало — возвращаем деньги
                log.info(" Компенсация: возврат средств для заказа {}", saga.getOrderId());
            }
            default -> log.info(" Компенсация: общий откат для шага {}", saga.getCurrentStep());
        }

        saga.setStatus(SagaStatus.COMPENSATED);
        sagaRepository.save(saga);

        log.info(" Сага компенсирована: id={}", saga.getId());
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Ошибка сериализации в JSON", e);
            return "{}";
        }
    }

    @Transactional
    public void onInventoryReserved(InventoryReservedEvent event) {
        log.info("Получен InventoryReservedEvent: sagaId={}", event.getSagaId());

        // Ищем сагу по sagaId, а не по orderId — событие приходит с sagaId
        SagaInstance saga = sagaRepository.findById(UUID.fromString(event.getSagaId()))
                .orElseThrow(() -> new RuntimeException(
                        "Сага не найдена: " + event.getSagaId()));

        if (saga.getCurrentStep() != SagaStep.INVENTORY_PROCESSING) {
            log.warn("Сага на шаге {}, ожидался INVENTORY_PROCESSING",
                    saga.getCurrentStep());
            return;
        }

        saga.setCurrentStep(SagaStep.INVENTORY_COMPLETED);
        saga.setStatus(SagaStatus.IN_PROGRESS);
        sagaRepository.save(saga);

        // Все шаги пройдены — завершаем сагу
        completeSaga(saga);
    }

    private OrderCreatedEvent parseOrderCreated(SagaInstance saga) {
        try {
            return objectMapper.readValue(saga.getPayload(), OrderCreatedEvent.class);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Не удалось распарсить payload саги " + saga.getId(), e);
        }
    }


}
