package com.example.sagaorchestrator.event;

import java.time.Instant;
import java.util.UUID;

public record SagaCompletedEvent(
        UUID sagaId,
        UUID orderId,
        Instant completedAt
) {}