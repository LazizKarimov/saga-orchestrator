package com.example.sagaorchestrator.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record RefundPaymentCommand(
        UUID sagaId,
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        String reason
) {}