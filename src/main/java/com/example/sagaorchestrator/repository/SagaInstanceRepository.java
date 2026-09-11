package com.example.sagaorchestrator.repository;

import com.example.sagaorchestrator.entity.SagaInstance;
import com.example.sagaorchestrator.entity.SagaStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByOrderId(UUID orderId);

    List<SagaInstance> findByStatus(SagaStatus status);

    boolean existsByOrderId(UUID orderId);
}