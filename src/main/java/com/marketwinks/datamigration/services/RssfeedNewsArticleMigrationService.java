
package com.marketwinks.datamigration.services;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.marketwinks.datamigration.model.Rssfeednewsarticles;
import com.marketwinks.datamigration.repository.RssfeedNewsArticleRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@RestController
@RequestMapping("/baseURL")
public class RssfeedNewsArticleMigrationService {

	@Autowired
	private RssfeedNewsArticleRepository RssfeedNewsArticleRepository;

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/rssfeednewsarticle_datamigrate/{Date}/{TimeinHour}/migrate", method = RequestMethod.GET)
	public boolean Rssfeednewsarticle_Migrator(@PathVariable String Date, @PathVariable String TimeinHour) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

			String endtimevalue = Date + "T" + TimeinHour + ":59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<Rssfeednewsarticles> MarketFeeds_full = mongoTemplate.find(query, Rssfeednewsarticles.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from rssfeednewsarticles: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "rssfeednewsarticles_historical");

			}

			mongoTemplate.remove(query, Rssfeednewsarticles.class);

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

}
