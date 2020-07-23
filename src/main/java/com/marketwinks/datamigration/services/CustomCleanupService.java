
package com.marketwinks.datamigration.services;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.marketwinks.datamigration.model.uk_lse_hourlybuys;
import com.marketwinks.datamigration.repository.UK_LSE_15MinBuyRepository;
import com.marketwinks.datamigration.repository.UK_LSE_15MinSellRepository;
import com.marketwinks.datamigration.repository.UK_LSE_30MinBuyRepository;
import com.marketwinks.datamigration.repository.UK_LSE_30MinSellRepository;
import com.marketwinks.datamigration.repository.UK_LSE_5MinBuyRepository;
import com.marketwinks.datamigration.repository.UK_LSE_5MinSellRepository;
import com.marketwinks.datamigration.repository.UK_LSE_DailyBuyRepository;
import com.marketwinks.datamigration.repository.UK_LSE_DailySellRepository;
import com.marketwinks.datamigration.repository.UK_LSE_HourlyBuyRepository;
import com.marketwinks.datamigration.repository.UK_LSE_HourlySellRepository;
import com.marketwinks.datamigration.repository.UK_LSE_MonthlyBuyRepository;
import com.marketwinks.datamigration.repository.UK_LSE_MonthlySellRepository;
import com.marketwinks.datamigration.repository.UK_LSE_WeeklyBuyRepository;
import com.marketwinks.datamigration.repository.UK_LSE_WeeklySellRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@RestController
@RequestMapping("/baseURL")
public class CustomCleanupService {

	@Autowired
	private UK_LSE_5MinBuyRepository UK_LSE_5MinBuyRepository;

	@Autowired
	private UK_LSE_15MinBuyRepository UK_LSE_15MinBuyRepository;

	@Autowired
	private UK_LSE_30MinBuyRepository UK_LSE_30MinBuyRepository;

	@Autowired
	private UK_LSE_HourlyBuyRepository UK_LSE_HourlyBuyRepository;

	@Autowired
	private UK_LSE_DailyBuyRepository UK_LSE_DailyBuyRepository;

	@Autowired
	private UK_LSE_WeeklyBuyRepository UK_LSE_WeeklyBuyRepository;

	@Autowired
	private UK_LSE_MonthlyBuyRepository UK_LSE_MonthlyBuyRepository;

	@Autowired
	private UK_LSE_5MinSellRepository UK_LSE_5MinSellRepository;

	@Autowired
	private UK_LSE_15MinSellRepository UK_LSE_15MinSellRepository;

	@Autowired
	private UK_LSE_30MinSellRepository UK_LSE_30MinSellRepository;

	@Autowired
	private UK_LSE_HourlySellRepository UK_LSE_HourlySellRepository;

	@Autowired
	private UK_LSE_DailySellRepository UK_LSE_DailySellRepository;

	@Autowired
	private UK_LSE_WeeklySellRepository UK_LSE_WeeklySellRepository;

	@Autowired
	private UK_LSE_MonthlySellRepository UK_LSE_MonthlySellRepository;

	@org.springframework.scheduling.annotation.Async
	@RequestMapping(value = "/customcleanup/delete", method = RequestMethod.GET)
	// public boolean Customcleanup(@PathVariable String Date) {
	public boolean Customcleanup() {
		System.gc();
		boolean execution_result = false;

		MongoClient mongoClient = MongoClients.create(
				"mongodb+srv://marketwinks:L9sS6oOAk1sHL0yi@aws-eu-west1-cluster-tszuq.mongodb.net/marketwinksdbprod?retryWrites=true");

		MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, "marketwinksdbprod");

		try {

			// DateTimeFormatter df1 =
			// DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			//
			//
			// Date = Date + "T23:59:59Z";
			//
			// LocalDateTime newLocalDate = LocalDateTime.parse(Date, df1);
			//
			// int from_days = 100;
			// int to_days = 10;

			// for (int d=from_days; d>=to_days; d--) {
			//
			// LocalDateTime newLocalDateStr = newLocalDate.minusDays(d);
			// DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			// String textdate = newLocalDateStr.format(formatters);
			//
			Query query = new Query();

			// query.addCriteria(Criteria.where("lastBuyEvent").regex(textdate));
			//
			//

			// query.addCriteria(Criteria.where("indicator").is("MACD"));

			// query.addCriteria(Criteria.where("company").is("AV."));
			// query.addCriteria(Criteria.where("company").is("BA."));
			query.addCriteria(Criteria.where("company").is("BP."));

			List<uk_lse_hourlybuys> MarketFeeds_full = mongoTemplate.find(query, uk_lse_hourlybuys.class);
			//
			// for (int i = 0; i < MarketFeeds_full.size(); i++) {
			// System.out.println("Migrating from uk_lse_5minbuys: " +
			// MarketFeeds_full.get(i).get_id());
			// mongoTemplate.insert(MarketFeeds_full.get(i), "uk_lse_5minbuys_historical");
			//
			// }

			mongoTemplate.remove(query, uk_lse_hourlybuys.class);
			// }

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
