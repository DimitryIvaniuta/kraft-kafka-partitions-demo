package com.github.dimitryivaniuta.kraftdemo.persistence.repo;

import com.github.dimitryivaniuta.kraftdemo.persistence.entity.BusinessEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface BusinessEventRepository extends JpaRepository<BusinessEvent, Long> {
    long countByEventId(UUID eventId);
}
