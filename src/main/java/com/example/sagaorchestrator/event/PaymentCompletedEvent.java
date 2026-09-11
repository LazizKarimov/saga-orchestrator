package com.example.sagaorchestrator.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaymentCompletedEvent {

    private UUID paymentId;
    @JsonProperty("orderId")
    private UUID orderId;
    private UUID customerId;
    private BigDecimal amount;
    private String status;
    private String eventType;
    private Long timestamp;
}