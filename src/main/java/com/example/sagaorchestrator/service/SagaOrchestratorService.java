package com.example.sagaorchestrator.service;

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

        saga = sagaRepository.save(saga);
        log.info(" Сага создана: id={}", saga.getId());

        // Переходим к шагу PAYMENT_PROCESSING
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga.setCurrentStep(SagaStep.PAYMENT_PROCESSING);
        sagaRepository.save(saga);

        // Отправляем команду на оплату
        sagaEventProducer.sendSagaEvent(SagaEvent.builder()
                .sagaId(saga.getId())
                .orderId(saga.getOrderId())
                .step(SagaStep.PAYMENT_PROCESSING.name())
                .status("COMMAND")
                .eventType("PROCESS_PAYMENT")
                .timestamp(System.currentTimeMillis())
                .build());
    }

    /**
     * Шаг 2: Получен PaymentCompletedEvent → двигаемся дальше
     */
    @Transactional
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info(" Получен PaymentCompletedEvent: orderId={}", event.getOrderId());

        SagaInstance saga = sagaRepository.findByOrderId(event.getOrderId())
                .orElseThrow(() -> new RuntimeException(
                        "Сага для заказа " + event.getOrderId() + " не найдена"));

        // Проверяем, что мы на правильном шаге
        if (saga.getCurrentStep() != SagaStep.PAYMENT_PROCESSING) {
            log.warn(" Сага на шаге {}, ожидался PAYMENT_PROCESSING",
                    saga.getCurrentStep());
            return;
        }

        // Обновляем шаг
        saga.setCurrentStep(SagaStep.PAYMENT_COMPLETED);
        saga.setStatus(SagaStatus.IN_PROGRESS);

        // Здесь можно добавить следующий шаг, например INVENTORY_RESERVING
        // Пока просто завершаем сагу
        completeSaga(saga);
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
                .orderId(saga.getId())
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


}
