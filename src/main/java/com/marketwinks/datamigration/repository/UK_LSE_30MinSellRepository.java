package com.marketwinks.datamigration.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.marketwinks.datamigration.model.uk_lse_30minsells;

@Repository
public interface UK_LSE_30MinSellRepository extends MongoRepository<uk_lse_30minsells, String> {
}