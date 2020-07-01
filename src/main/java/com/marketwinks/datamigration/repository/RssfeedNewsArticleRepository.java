package com.marketwinks.datamigration.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.marketwinks.datamigration.model.Rssfeednewsarticles;

@Repository
public interface RssfeedNewsArticleRepository extends MongoRepository<Rssfeednewsarticles, String> {
}