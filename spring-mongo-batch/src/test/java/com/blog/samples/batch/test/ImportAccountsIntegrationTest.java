package com.blog.samples.batch.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import br.com.javatar.batch.Account;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/batch-config.xml" })
public class ImportAccountsIntegrationTest
{

	@Autowired
	private JobLauncher jobLauncher_i;
	
	@Autowired
	private Job job_i;
	
	@Value("file:src/test/resources/input/accounts.txt")
	private Resource accountsResource;
	
	@Value("file:src/test/resources/input/accountsError.txt")
	private Resource accountsErrorResource;
	
	@Autowired
	private MongoTemplate mongoTemplate;
	
	@Before
	public void setUp() throws Exception
	{
		mongoTemplate.dropCollection("account");
	}

	@Test
	public void importAccountDataTest() throws Exception
	{
		long startingCount = mongoTemplate.count(new Query(), Account.class);
		jobLauncher_i.run(job_i, new JobParametersBuilder().addString("inputResource", accountsResource.getFile().getAbsolutePath())
															.addLong("timestamp", System.currentTimeMillis())
															.toJobParameters());

		int accountsAdded = 10;
		Assert.assertEquals(startingCount + accountsAdded, mongoTemplate.count(new Query(), Account.class));
	}

	@Test
	public void importAccountDataErrorTest() throws Exception
	{
		long startingCount = mongoTemplate.count(new Query(), Account.class);
		jobLauncher_i.run(job_i, new JobParametersBuilder().addString("inputResource", accountsErrorResource.getFile().getAbsolutePath())
															.addLong("timestamp", System.currentTimeMillis())
															.toJobParameters());

		int accountsAdded = 8;
		Assert.assertEquals(startingCount + accountsAdded, mongoTemplate.count(new Query(), Account.class));
	}
}