package br.com.javatar.batch;

import java.io.Serializable;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;

import com.mongodb.DBCollection;

/** Base class for all MongoDB DAO implementation 
 * @param <T>*/
public abstract class AbstractMongoDbDao<T, K> extends SimpleMongoRepository<T, Serializable> implements InitializingBean {
    
    public AbstractMongoDbDao(MongoEntityInformation<T, Serializable> metadata, MongoOperations mongoOperations) {
		super(metadata, mongoOperations);
	}

	public static final String DEFAULT_COLLECTION_PREFIX = "Batch";

    public static final int DEFAULT_EXIT_MESSAGE_LENGTH = 2500;

    protected static final String UPDATED_EXISTING_STATUS = "updatedExisting";

    protected static final String VERSION_KEY = "version";

    protected static final String START_TIME_KEY = "startTime";

    protected static final String END_TIME_KEY = "endTime";

    protected static final String EXIT_CODE_KEY = "exitCode";

    protected static final String EXIT_MESSAGE_KEY = "exitMessage";

    protected static final String LAST_UPDATED_KEY = "lastUpdated";

    protected static final String STATUS_KEY = "status";

    protected static final String JOB_EXECUTION_ID_KEY = "jobExecutionId";

    protected static final String JOB_INSTANCE_ID_KEY = "jobInstanceId";

    protected static final String STEP_EXECUTION_ID_KEY = "stepExecutionId";

    protected static final String JOB_NAME_KEY = "jobName";

    protected String prefix = DEFAULT_COLLECTION_PREFIX;

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
	protected DBCollection getCollection() {
		return getMongoOperations().getCollection(getEntityInformation().getCollectionName());
	}
	
    protected String getCollectionName()
    {
        return getEntityInformation().getCollectionName();
    }

    public void afterPropertiesSet() throws Exception {
    }

}
