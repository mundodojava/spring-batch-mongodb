package br.com.javatar.batch;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * Class takes Account model objects created in item reader and makes them
 * available to writer to persist in the database
 * 
 */
public class AccountItemWriter implements ItemWriter<Account> {

	@Autowired
	private MongoTemplate mongoTemplate;

	/**
	 * Method takes a list of Account model objects and uses JDBC template to
	 * either insert or update them in the database
	 */
	public void write(List<? extends Account> accounts_p) throws Exception {
		for (Account account : accounts_p) {
			mongoTemplate.save(account);
		}
	}
}