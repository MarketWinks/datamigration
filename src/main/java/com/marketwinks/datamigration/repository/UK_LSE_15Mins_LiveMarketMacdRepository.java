package com.marketwinks.datamigration.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.marketwinks.datamigration.model.uk_lse_15mins_livemarketmacd;

@Repository
public interface UK_LSE_15Mins_LiveMarketMacdRepository extends MongoRepository<uk_lse_15mins_livemarketmacd, String> {
}
