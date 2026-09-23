package com.example.sagaorchestrator.service;

import com.example.sagaorchestrator.config.SagaCommandProducer;
import com.example.sagaorchestrator.dto.*;
import com.example.sagaorchestrator.entity.SagaInstance;
import com.example.sagaorchestrator.entity.SagaStatus;
import com.example.sagaorchestrator.entity.SagaStep;
import com.example.sagaorchestrator.event.OrderCancelledEvent;
import com.example.sagaorchestrator.event.OrderCreatedEvent;
import com.example.sagaorchestrator.event.PaymentCompletedEvent;
import com.example.sagaorchestrator.event.SagaCompletedEvent;
import com.example.sagaorchestrator.kafka.producer.SagaEventProducer;
import com.example.sagaorchestrator.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
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
    private final SagaCommandProducer sagaCommandProducer;

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
        ProcessPaymentCommand command = new ProcessPaymentCommand(
                saga.getId(),
                saga.getOrderId(),
                saga.getCustomerId(),
                event.getAmount()
        );
        sagaCommandProducer.sendProcessPaymentCommand(command);
    }

    @Transactional
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Получен PaymentCompletedEvent: sagaId={}", event.sagaId());

        Optional<SagaInstance> maybeSaga = sagaRepository.findById(event.sagaId());
        if (maybeSaga.isEmpty()) {
            log.warn("Сага {} не найдена, событие игнорируется", event.sagaId());
            return;
        }
        SagaInstance saga = maybeSaga.get();

        if (saga.getCurrentStep() != SagaStep.PAYMENT_PROCESSING) {
            log.warn("Сага на шаге {}, ожидался PAYMENT_PROCESSING", saga.getCurrentStep());
            return;
        }

        saga.setPaymentId(event.paymentId());
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

        sagaCommandProducer.sendReserveInventoryCommand(command);
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

        log.info("Сага завершена: id={}, orderId={}", saga.getId(), saga.getOrderId());

        sagaEventProducer.sendSagaCompletedEvent(new SagaCompletedEvent(
                saga.getId(),
                saga.getOrderId(),
                Instant.now()
        ));
    }

    @Transactional
    public void onPaymentRefunded(PaymentRefundedEvent event) {
        log.info("Получен PaymentRefundedEvent: sagaId={}, paymentId={}",
                event.sagaId(), event.paymentId());

        Optional<SagaInstance> maybeSaga = sagaRepository.findById(event.sagaId());
        if (maybeSaga.isEmpty()) {
            log.warn("Сага {} не найдена, событие игнорируется", event.sagaId());
            return;
        }
        SagaInstance saga = maybeSaga.get();

        if (saga.getCurrentStep() != SagaStep.REFUNDING_PAYMENT) {
            log.warn("Сага на шаге {}, ожидался REFUNDING_PAYMENT, событие игнорируется",
                    saga.getCurrentStep());
            return;
        }

        saga.setCurrentStep(SagaStep.CANCELLING_ORDER);
        saga.setStatus(SagaStatus.COMPENSATING);
        sagaRepository.save(saga);

        CancelOrderCommand command = new CancelOrderCommand(
                saga.getId(),
                saga.getOrderId(),
                saga.getErrorMessage()
        );
        sagaCommandProducer.sendCancelOrderCommand(command);

        log.info("Сага {} переведена в CANCELLING_ORDER, отправлен CancelOrderCommand",
                saga.getId());
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

    @Transactional
    public void onOrderCancelled(OrderCancelledEvent event) {
        log.info("Получен OrderCancelledEvent: sagaId={}, orderId={}",
                event.sagaId(), event.orderId());

        Optional<SagaInstance> maybeSaga = sagaRepository.findById(event.sagaId());
        if (maybeSaga.isEmpty()) {
            log.warn("Сага {} не найдена, событие игнорируется", event.sagaId());
            return;
        }
        SagaInstance saga = maybeSaga.get();

        if (saga.getCurrentStep() != SagaStep.CANCELLING_ORDER) {
            log.warn("Сага на шаге {}, ожидался CANCELLING_ORDER, событие игнорируется",
                    saga.getCurrentStep());
            return;
        }

        saga.setCurrentStep(SagaStep.ORDER_CANCELLED);
        sagaRepository.save(saga);

        log.info("Сага {} — заказ отменён. Дальше: финализация компенсации (подэтап 4).",
                saga.getId());
        // TODO (подэтап 4): saga.setStatus(COMPENSATED); publish SagaCompensatedEvent
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

    @Transactional
    public void onInventoryReservationFailed(InventoryReservationFailedEvent event) {
        log.warn("Получен InventoryReservationFailedEvent: sagaId={}, reason={}",
                event.sagaId(), event.errorMessage());

        Optional<SagaInstance> maybeSaga = sagaRepository.findById(event.sagaId());
        if (maybeSaga.isEmpty()) {
            log.warn("Сага {} не найдена, событие игнорируется", event.sagaId());
            return;
        }
        SagaInstance saga = maybeSaga.get();

        if (saga.getCurrentStep() != SagaStep.INVENTORY_PROCESSING) {
            log.warn("Сага на шаге {}, ожидался INVENTORY_PROCESSING, событие игнорируется",
                    saga.getCurrentStep());
            return;
        }

        saga.setCurrentStep(SagaStep.REFUNDING_PAYMENT);
        saga.setStatus(SagaStatus.COMPENSATING);
        saga.setErrorMessage(event.errorMessage());
        sagaRepository.save(saga);

        RefundPaymentCommand command = new RefundPaymentCommand(
                saga.getId(),
                saga.getOrderId(),
                saga.getPaymentId(),
                null,
                event.errorMessage()
        );
        sagaCommandProducer.sendRefundPaymentCommand(command);

        log.warn("Сага {} переведена в REFUNDING_PAYMENT, отправлен RefundPaymentCommand",
                saga.getId());
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
