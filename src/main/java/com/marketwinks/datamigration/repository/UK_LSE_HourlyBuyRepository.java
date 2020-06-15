package com.marketwinks.datamigration.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.marketwinks.datamigration.model.uk_lse_hourlybuys;

@Repository
public interface UK_LSE_HourlyBuyRepository extends MongoRepository<uk_lse_hourlybuys, String> {
}