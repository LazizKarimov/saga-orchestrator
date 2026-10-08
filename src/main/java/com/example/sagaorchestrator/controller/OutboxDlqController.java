package com.example.sagaorchestrator.controller;

import com.example.sagaorchestrator.entity.OutboxDlqEvent;
import com.example.sagaorchestrator.entity.OutboxEvent;
import com.example.sagaorchestrator.repository.OutboxDlqRepository;
import com.example.sagaorchestrator.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/admin/dlq")
@RequiredArgsConstructor
@Slf4j
public class OutboxDlqController {

    private final OutboxDlqRepository dlqRepository;
    private final OutboxEventRepository outboxRepository;

    @GetMapping
    public List<OutboxDlqEvent> list() {
        return dlqRepository.findAllByOrderByFailedAtDesc();
    }

    @PostMapping("/{id}/retry")
    @Transactional
    public ResponseEntity<String> retry(@PathVariable UUID id) {
        OutboxDlqEvent dlq = dlqRepository.findById(id).orElse(null);
        if (dlq == null) {
            return ResponseEntity.notFound().build();
        }

        // Возвращаем в outbox_events с attempts = 0
        outboxRepository.save(OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateId(dlq.getAggregateId())
                .eventType(dlq.getEventType())
                .topic(dlq.getTopic())
                .payload(dlq.getPayload())
                .createdAt(LocalDateTime.now())
                .attempts(0)
                .build());

        dlqRepository.delete(dlq);
        log.info("DLQ запись {} возвращена в outbox для повторной отправки", id);

        return ResponseEntity.ok("Запись " + id + " возвращена в outbox");
    }
}