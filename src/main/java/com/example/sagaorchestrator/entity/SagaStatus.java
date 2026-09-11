package com.example.sagaorchestrator.entity;

public enum SagaStatus {
    STARTED,          // Сага запущена
    IN_PROGRESS,      // Выполняется шаг
    COMPLETED,        // Все шаги успешно
    COMPENSATING,     // Откат
    COMPENSATED,      // Откат завершён
    FAILED            // Ошибка без возможности отката
}
