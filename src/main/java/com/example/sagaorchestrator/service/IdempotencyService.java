package com.example.sagaorchestrator.service;

import com.example.sagaorchestrator.entity.ProcessedEvent;
import com.example.sagaorchestrator.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private final ProcessedEventRepository processedEventRepository;

    /**
     * Пытается пометить eventId как обработанный.
     * @return true — обрабатывается впервые, false — дубликат.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean tryMarkProcessed(UUID eventId, String consumer, String eventType) {
        if (eventId == null) {
            throw new IllegalArgumentException(
                    "eventId не может быть null (eventType=" + eventType + ")");
        }

        if (processedEventRepository.existsById(eventId)) {
            log.warn("Дубликат события: eventId={}, consumer={}, eventType={}",
                    eventId, consumer, eventType);
            return false;
        }

        try {
            processedEventRepository.save(ProcessedEvent.builder()
                    .eventId(eventId)
                    .consumer(consumer)
                    .eventType(eventType)
                    .processedAt(LocalDateTime.now())
                    .build());
            return true;
        } catch (DataIntegrityViolationException e) {
            log.warn("Гонка на eventId={}, consumer={}, eventType={}",
                    eventId, consumer, eventType);
            return false;
        }
    }
}