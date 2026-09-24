package com.example.sagaorchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /**
     * Идентификатор агрегата. У нас это sagaId — события одной саги
     * должны попадать в одну партицию Kafka при отправке.
     */
    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private String aggregateId;

    /**
     * Логический тип события: RefundPaymentCommand, ReserveInventoryCommand и т.д.
     * Не путать с Java-классом — это просто строка для читаемости и фильтрации.
     */
    @Column(name = "event_type", nullable = false, updatable = false)
    private String eventType;

    /**
     * Kafka topic, куда надо отправить payload.
     */
    @Column(name = "topic", nullable = false, updatable = false)
    private String topic;

    /**
     * Сериализованный JSON payload'а сообщения.
     */
    @Column(name = "payload", nullable = false, updatable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /**
     * null = ещё не отправлено в Kafka.
     * not null = отправлено успешно, время отправки.
     */
    @Column(name = "processed_at")
    private LocalDateTime processedAt;
}