
package com.marketwinks.datamigration.services;

import java.text.SimpleDateFormat;
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

import com.marketwinks.datamigration.model.uk_lse_15minsells;
import com.marketwinks.datamigration.model.uk_lse_15mins_livemarketmacd;
import com.marketwinks.datamigration.model.uk_lse_30minsells;
import com.marketwinks.datamigration.model.uk_lse_30mins_livemarketmacd;
import com.marketwinks.datamigration.model.uk_lse_5minsells;
import com.marketwinks.datamigration.model.uk_lse_5mins_livemarketmacd;
import com.marketwinks.datamigration.model.uk_lse_daily_livemarketmacd;
import com.marketwinks.datamigration.model.uk_lse_dailysells;
import com.marketwinks.datamigration.model.uk_lse_hourly_livemarketmacd;
import com.marketwinks.datamigration.model.uk_lse_hourlysells;
import com.marketwinks.datamigration.model.uk_lse_monthly_livemarketmacd;
import com.marketwinks.datamigration.model.uk_lse_monthlysells;
import com.marketwinks.datamigration.model.uk_lse_weekly_livemarketmacd;
import com.marketwinks.datamigration.model.uk_lse_weeklysells;
import com.marketwinks.datamigration.repository.UK_LSE_15Mins_LiveMarketMacdRepository;
import com.marketwinks.datamigration.repository.UK_LSE_30Mins_LiveMarketMacdRepository;
import com.marketwinks.datamigration.repository.UK_LSE_5MinSellRepository;
import com.marketwinks.datamigration.repository.UK_LSE_5Mins_LiveMarketMacdRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Daily_LiveMarketMacdRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Hourly_LiveMarketMacdRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Monthly_LiveMarketMacdRepository;
import com.marketwinks.datamigration.repository.UK_LSE_Weekly_LiveMarketMacdRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@RestController
@RequestMapping("/baseURL")
public class SellSignalsMigrationService {

	@Autowired
	private UK_LSE_5MinSellRepository UK_LSE_5MinSellRepository;

	@Autowired
	private UK_LSE_15Mins_LiveMarketMacdRepository UK_LSE_15Mins_LiveMarketMacdRepository;

	@Autowired
	private UK_LSE_30Mins_LiveMarketMacdRepository UK_LSE_30Mins_LiveMarketMacdRepository;

	@Autowired
	private UK_LSE_Hourly_LiveMarketMacdRepository UK_LSE_Hourly_LiveMarketMacdRepository;

	@Autowired
	private UK_LSE_Daily_LiveMarketMacdRepository UK_LSE_Daily_LiveMarketMacdRepository;

	@Autowired
	private UK_LSE_Weekly_LiveMarketMacdRepository UK_LSE_Weekly_LiveMarketMacdRepository;

	@Autowired
	private UK_LSE_Monthly_LiveMarketMacdRepository UK_LSE_Monthly_LiveMarketMacdRepository;

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/uk_sell_signals_5Mins_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_SellSignals_5Min_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			

			Date = Date + "T23:59:59Z";

			LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			
			int from_days = 100;
			int to_days = 1;
			
			for (int d=from_days; d>=to_days; d--) {
				
				LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
				 DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
				    String textdate = newLocalDateStr.format(formatters);
				    
			Query query = new Query();
			query.addCriteria(Criteria.where("lastSellEvent").regex(textdate));
			
			query.addCriteria(Criteria.where("indicator").is("EXPIRED"));
			
			List<uk_lse_5minsells> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_5minsells.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_5minsells: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_5minsells_historical");

			}

			mongoTemplate.remove(query, uk_lse_5minsells.class);
			}
			

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
	@RequestMapping(value = "/uk_sell_signals_15Mins_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_SellSignals_15Min_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			

			Date = Date + "T23:59:59Z";

			LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			
			int from_days = 100;
			int to_days = 2;
			
			for (int d=from_days; d>=to_days; d--) {
				
				LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
				 DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
				    String textdate = newLocalDateStr.format(formatters);
				    
			Query query = new Query();
			query.addCriteria(Criteria.where("lastSellEvent").regex(textdate));
			
			query.addCriteria(Criteria.where("indicator").is("EXPIRED"));
			
			List<uk_lse_15minsells> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_15minsells.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_15minsells: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_15minsells_historical");

			}

			mongoTemplate.remove(query, uk_lse_15minsells.class);
			}
			

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
	@RequestMapping(value = "/uk_sell_signals_30Mins_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_SellSignals_30Min_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			

			Date = Date + "T23:59:59Z";

			LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			
			int from_days = 100;
			int to_days = 3;
			
			for (int d=from_days; d>=to_days; d--) {
				
				LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
				 DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
				    String textdate = newLocalDateStr.format(formatters);
				    
			Query query = new Query();
			query.addCriteria(Criteria.where("lastSellEvent").regex(textdate));
			
			query.addCriteria(Criteria.where("indicator").is("EXPIRED"));
			
			List<uk_lse_30minsells> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_30minsells.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_30minsells: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_30minsells_historical");

			}

			mongoTemplate.remove(query, uk_lse_30minsells.class);
			}
			

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
	@RequestMapping(value = "/uk_sell_signals_Hourly_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_SellSignals_Hourly_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			

			Date = Date + "T23:59:59Z";

			LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			
			int from_days = 100;
			int to_days = 4;
			
			for (int d=from_days; d>=to_days; d--) {
				
				LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
				 DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
				    String textdate = newLocalDateStr.format(formatters);
				    
			Query query = new Query();
			query.addCriteria(Criteria.where("lastSellEvent").regex(textdate));
			
			query.addCriteria(Criteria.where("indicator").is("EXPIRED"));
			
			List<uk_lse_hourlysells> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_hourlysells.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_hourlysells: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_hourlysells_historical");

			}

			mongoTemplate.remove(query, uk_lse_hourlysells.class);
			}
			

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
	@RequestMapping(value = "/uk_sell_signals_daily_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_SellSignals_Daily_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			

			Date = Date + "T23:59:59Z";

			LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			
			int from_days = 200;
			int to_days = 5;
			
			for (int d=from_days; d>=to_days; d--) {
				
				LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
				 DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
				    String textdate = newLocalDateStr.format(formatters);
				    
			Query query = new Query();
			query.addCriteria(Criteria.where("lastSellEvent").regex(textdate));
			
			query.addCriteria(Criteria.where("indicator").is("EXPIRED"));
			
			List<uk_lse_dailysells> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_dailysells.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_dailysells: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_dailysells_historical");

			}

			mongoTemplate.remove(query, uk_lse_dailysells.class);
			}
			

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
	@RequestMapping(value = "/uk_sell_signals_weekly_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_SellSignals_Weekly_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			

			Date = Date + "T23:59:59Z";

			LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			
			int from_days = 400;
			int to_days = 10;
			
			for (int d=from_days; d>=to_days; d--) {
				
				LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
				 DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
				    String textdate = newLocalDateStr.format(formatters);
				    
			Query query = new Query();
			query.addCriteria(Criteria.where("lastSellEvent").regex(textdate));
			
			query.addCriteria(Criteria.where("indicator").is("EXPIRED"));
			
			List<uk_lse_weeklysells> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_weeklysells.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_weeklysells: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_weeklysells_historical");

			}

			mongoTemplate.remove(query, uk_lse_weeklysells.class);
			}
			

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
	@RequestMapping(value = "/uk_sell_signals_monthly_datamigrate/{Date}/migrate", method = RequestMethod.GET)
	public boolean UK_SellSignals_Monthly_Migrator(@PathVariable String Date) {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			DateTimeFormatter df1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			

			Date = Date + "T23:59:59Z";

			LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			
			int from_days = 1000;
			int to_days = 20;
			
			for (int d=from_days; d>=to_days; d--) {
				
				LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
				 DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
				    String textdate = newLocalDateStr.format(formatters);
				    
			Query query = new Query();
			query.addCriteria(Criteria.where("lastSellEvent").regex(textdate));
			
			query.addCriteria(Criteria.where("indicator").is("EXPIRED"));
			
			List<uk_lse_monthlysells> MarketFeeds_full = mongoTemplate.find(query,
					uk_lse_monthlysells.class);

			for (int i = 0; i < MarketFeeds_full.size(); i++) {
				System.out.println("Migrating from uk_lse_monthlysells: " + MarketFeeds_full.get(i).get_id());
				mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_monthlysells_historical");

			}

			mongoTemplate.remove(query, uk_lse_monthlysells.class);
			}
			

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
