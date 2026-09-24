package com.example.sagaorchestrator.repository;

import com.example.sagaorchestrator.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query(value = """
            SELECT * FROM outbox_events
            WHERE processed_at IS NULL
            ORDER BY created_at
            LIMIT :batchSize
            """, nativeQuery = true)
    List<OutboxEvent> findUnprocessedBatch(@Param("batchSize") int batchSize);
}