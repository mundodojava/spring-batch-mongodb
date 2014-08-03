package br.com.javatar.batch;

import static com.mongodb.BasicDBObjectBuilder.start;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/** {@link org.springframework.batch.core.repository.dao.JobInstanceDao} implementation for MongoDB */
public class MongoDbJobInstanceDao extends AbstractMongoDbDao<JobInstance,Long> implements JobInstanceDao {
	
	static final String COLLECTION_NAME = JobInstance.class.getSimpleName();
	
	private static final String JOB_KEY_KEY = "jobKey";
    
	public MongoDbJobInstanceDao(MongoEntityInformation<JobInstance, Serializable> metadata,
			MongoOperations mongoOperations) {
		super(metadata, mongoOperations);
	}

    private JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    private ValueIncrementer jobIncrementer;

    public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        Assert.state(getJobInstance(jobName, jobParameters) == null, "JobInstance must not already exist");

        Long jobId = jobIncrementer.nextLongValue();

        JobInstance jobInstance = new JobInstance(jobId, jobName);
        jobInstance.incrementVersion();

        getCollection().save(
            start().add(JOB_INSTANCE_ID_KEY, jobId).add(JOB_NAME_KEY, jobName).add(JOB_KEY_KEY, jobKeyGenerator.generateKey(jobParameters)).add(VERSION_KEY, jobInstance.getVersion()).get());

        return jobInstance;
    }

    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        String jobKey = jobKeyGenerator.generateKey(jobParameters);

        return mapJobInstance(getCollection().findOne(start().add(JOB_NAME_KEY, jobName).add(JOB_KEY_KEY, jobKey).get()));
    }

    public JobInstance getJobInstance(Long instanceId) {
        return mapJobInstance(getCollection().findOne(new BasicDBObject(JOB_INSTANCE_ID_KEY, instanceId)));
    }

    public JobInstance getJobInstance(JobExecution jobExecution) {
        DBObject instanceId = getCollection().findOne(new BasicDBObject(JOB_EXECUTION_ID_KEY, jobExecution.getId()), new BasicDBObject(JOB_INSTANCE_ID_KEY, 1L));
        return mapJobInstance(getCollection().findOne(new BasicDBObject(JOB_INSTANCE_ID_KEY, instanceId.get(JOB_INSTANCE_ID_KEY))));
    }

    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return mapJobInstances(getCollection().find(new BasicDBObject(JOB_NAME_KEY, jobName)).sort(new BasicDBObject(JOB_INSTANCE_ID_KEY, -1L)).skip(start).limit(count));
    }

    public List<String> getJobNames() {
        @SuppressWarnings("unchecked")
        List<String> results = getCollection().distinct(JOB_NAME_KEY);
        Collections.sort(results);
        return results;
    }

    private List<JobInstance> mapJobInstances(DBCursor dbCursor) {
        List<JobInstance> results = new ArrayList<JobInstance>();
        while (dbCursor.hasNext()) {
            results.add(mapJobInstance(dbCursor.next()));
        }
        dbCursor.close();
        return results;
    }

    private JobInstance mapJobInstance(DBObject dbObject) {
        if (dbObject == null) {
            return null;
        }
        JobInstance jobInstance = new JobInstance((Long) dbObject.get(JOB_INSTANCE_ID_KEY), (String) dbObject.get(JOB_NAME_KEY));
        // should always be at version=0 because they never get updated
        jobInstance.incrementVersion();
        return jobInstance;
    }

    public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        return getJobInstances(jobName, start, count);
    }

    public int getJobInstanceCount(String jobName) throws NoSuchJobException {
    	
    	Query query = new Query();
    	query.addCriteria(Criteria.where(JOB_NAME_KEY).is(jobName));
        return Long.valueOf(getMongoOperations().count(query, getCollectionName())).intValue();
    }

    public void setJobIncrementer(ValueIncrementer jobIncrementer) {
        this.jobIncrementer = jobIncrementer;
    }

}
