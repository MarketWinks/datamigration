
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

import com.marketwinks.datamigration.model.uk_lse_15mins_livemarketprice;
import com.marketwinks.datamigration.model.uk_lse_30mins_livemarketprice;
import com.marketwinks.datamigration.model.uk_lse_5mins_livemarketprice;
import com.marketwinks.datamigration.model.uk_lse_daily_livemarketprice;
import com.marketwinks.datamigration.model.uk_lse_hourly_livemarketprice;
import com.marketwinks.datamigration.model.uk_lse_monthly_livemarketprice;
import com.marketwinks.datamigration.model.uk_lse_weekly_livemarketprice;
import com.marketwinks.datamigration.repository.UK_LSE_15Mins_LiveMarketPriceRepository;
import com.marketwinks.datamigration.repository.UK_LSE_30Mins_LiveMarketPriceRepository;
import com.marketwinks.datamigration.repository.UK_LSE_5Mins_LiveMarketPriceRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Daily_LiveMarketPriceRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Hourly_LiveMarketPriceRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Monthly_LiveMarketPriceRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Weekly_LiveMarketPriceRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@RestController
@RequestMapping("/baseURL")
public class LiveMarketPriceMigrationService {

	@Autowired
	private UK_LSE_5Mins_LiveMarketPriceRepository UK_LSE_5Mins_LiveMarketPriceRepository;

	@Autowired
	private UK_LSE_15Mins_LiveMarketPriceRepository UK_LSE_15Mins_LiveMarketPriceRepository;

	@Autowired
	private UK_LSE_30Mins_LiveMarketPriceRepository UK_LSE_30Mins_LiveMarketPriceRepository;

	@Autowired
	private UK_LSE_Hourly_LiveMarketPriceRepository UK_LSE_Hourly_LiveMarketPriceRepository;

	@Autowired
	private UK_LSE_Daily_LiveMarketPriceRepository UK_LSE_Daily_LiveMarketPriceRepository;

	@Autowired
	private UK_LSE_Weekly_LiveMarketPriceRepository UK_LSE_Weekly_LiveMarketPriceRepository;

	@Autowired
	private UK_LSE_Monthly_LiveMarketPriceRepository UK_LSE_Monthly_LiveMarketPriceRepository;

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_lse_5mins_price_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_LSE_5Mins_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

			String endtimevalue = Date + "T23:59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<uk_lse_5mins_livemarketprice> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_5mins_livemarketprice.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_5mins_livemarketprice: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_5mins_livemarketprice_historical");

			}

			mongoTemplate.remove(query, uk_lse_5mins_livemarketprice.class);

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_lse_15mins_price_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_LSE_15Mins_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			String endtimevalue = Date + "T23:59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<uk_lse_15mins_livemarketprice> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_15mins_livemarketprice.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_15mins_livemarketprice: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_15mins_livemarketprice_historical");

			}

			mongoTemplate.remove(query, uk_lse_15mins_livemarketprice.class);

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_lse_30mins_price_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_LSE_30Mins_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

			String endtimevalue = Date + "T23:59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<uk_lse_30mins_livemarketprice> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_30mins_livemarketprice.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_30mins_livemarketprice: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_30mins_livemarketprice_historical");

			}

			mongoTemplate.remove(query, uk_lse_30mins_livemarketprice.class);

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_lse_hourly_price_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_LSE_Hourly_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			String endtimevalue = Date + "T23:59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<uk_lse_hourly_livemarketprice> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_hourly_livemarketprice.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_hourly_livemarketprice: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_hourly_livemarketprice_historical");

			}

			mongoTemplate.remove(query, uk_lse_hourly_livemarketprice.class);

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_lse_daily_price_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_LSE_Daily_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

			String endtimevalue = Date + "T23:59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<uk_lse_daily_livemarketprice> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_daily_livemarketprice.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_daily_livemarketprice: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_daily_livemarketprice_historical");

			}

			mongoTemplate.remove(query, uk_lse_daily_livemarketprice.class);

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_lse_weekly_price_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_LSE_Weekly_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

			String endtimevalue = Date + "T23:59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<uk_lse_weekly_livemarketprice> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_weekly_livemarketprice.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_weekly_livemarketprice: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_weekly_livemarketprice_historical");

			}

			mongoTemplate.remove(query, uk_lse_weekly_livemarketprice.class);

			execution_result = true;
		} catch (Exception e) {

			System.out.println(e);
		} finally {

			mongoClient.close();

			System.gc();

		}

		return execution_result;

	}

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_lse_monthly_price_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_LSE_Monthly_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

			String endtimevalue = Date + "T23:59:59Z";
			LocalDateTime newLocalDateTime_endtimevalue = LocalDateTime.parse(endtimevalue, df1);

			Query query = new Query();
			query.addCriteria(Criteria.where("time").lt(newLocalDateTime_endtimevalue));

			List<uk_lse_monthly_livemarketprice> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_monthly_livemarketprice.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out
						.println("Migrating from uk_lse_monthly_livemarketprice: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_monthly_livemarketprice_historical");

			}

			mongoTemplate.remove(query, uk_lse_monthly_livemarketprice.class);

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
