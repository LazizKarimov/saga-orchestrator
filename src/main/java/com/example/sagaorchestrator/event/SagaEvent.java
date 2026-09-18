package com.example.sagaorchestrator.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SagaEvent {

    private UUID sagaId;
    private UUID orderId;
    private BigDecimal amount;
    private UUID customerId;
    private String step;
    private String status;
    private String eventType;
    private Long timestamp;
}