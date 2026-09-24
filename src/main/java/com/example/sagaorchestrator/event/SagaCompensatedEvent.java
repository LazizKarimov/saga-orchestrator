package com.example.sagaorchestrator.event;

import java.time.Instant;
import java.util.UUID;

public record SagaCompensatedEvent(
        UUID sagaId,
        UUID orderId,
        String reason,
        Instant compensatedAt
) {}
