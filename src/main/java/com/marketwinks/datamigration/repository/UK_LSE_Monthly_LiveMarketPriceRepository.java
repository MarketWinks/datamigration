package com.marketwinks.datamigration.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.marketwinks.datamigration.model.uk_lse_monthly_livemarketprice;

@Repository
public interface UK_LSE_Monthly_LiveMarketPriceRepository extends MongoRepository<uk_lse_monthly_livemarketprice, String> {
}
