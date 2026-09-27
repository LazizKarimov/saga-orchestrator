package com.example.sagaorchestrator.event;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentCompletedEvent(
        UUID eventId,
        UUID sagaId,
        UUID paymentId,
        UUID orderId,
        BigDecimal amount
) {}