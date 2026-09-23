package com.example.sagaorchestrator.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentRefundedEvent(
        UUID sagaId,
        UUID paymentId,
        UUID orderId,
        BigDecimal amount,
        Instant refundedAt
) {}