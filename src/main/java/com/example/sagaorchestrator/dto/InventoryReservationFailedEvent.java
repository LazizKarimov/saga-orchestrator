package com.example.sagaorchestrator.dto;

import java.util.UUID;

public record InventoryReservationFailedEvent(
        UUID sagaId,
        UUID orderId,
        String errorMessage
) {}