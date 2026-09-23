package com.example.sagaorchestrator.dto;

import java.util.UUID;

public record CancelOrderCommand(
        UUID sagaId,
        UUID orderId,
        String reason
) {}