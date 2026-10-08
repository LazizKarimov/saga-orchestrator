package com.example.sagaorchestrator.repository;

import com.example.sagaorchestrator.entity.OutboxDlqEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxDlqRepository extends JpaRepository<OutboxDlqEvent, UUID> {

    List<OutboxDlqEvent> findAllByOrderByFailedAtDesc();
}