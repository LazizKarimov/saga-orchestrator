package com.example.sagaorchestrator.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record ProcessPaymentCommand(
        UUID sagaId,
        UUID orderId,
        UUID customerId,
        BigDecimal amount
) {}