package com.example.sagaorchestrator.entity;

public enum SagaStep {
    ORDER_CREATED,         // Заказ создан
    PAYMENT_PROCESSING,    // Ожидание платежа
    PAYMENT_COMPLETED,     // Платёж успешен
    INVENTORY_RESERVING,   // Резервирование товара
    INVENTORY_RESERVED,    // Товар зарезервирован
    SHIPPING_STARTED,      // Доставка начата
    COMPLETED              // Сага завершена
}
