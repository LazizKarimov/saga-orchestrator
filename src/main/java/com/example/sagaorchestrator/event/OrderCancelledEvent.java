package com.example.sagaorchestrator.event;

import java.time.Instant;
import java.util.UUID;

public record OrderCancelledEvent(
        UUID eventId,
        UUID sagaId,
        UUID orderId,
        Instant cancelledAt
) {}