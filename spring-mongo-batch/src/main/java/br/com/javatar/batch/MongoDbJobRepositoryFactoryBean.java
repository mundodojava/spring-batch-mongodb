package br.com.javatar.batch;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.dao.XStreamExecutionContextStringSerializer;
import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.MappingMongoEntityInformation;
import org.springframework.util.Assert;

import com.mongodb.DB;

/**
 * A {@link org.springframework.beans.factory.FactoryBean} that automates the creation of a {@link org.springframework.batch.core.repository.support.SimpleJobRepository} with MongoDB dao.
 */
public class MongoDbJobRepositoryFactoryBean extends AbstractJobRepositoryFactoryBean implements FactoryBean<JobRepository>, InitializingBean {

    protected static final Log logger = LogFactory.getLog(MongoDbJobRepositoryFactoryBean.class);

    private MongoTemplate mongoTemplate;

    private String collectionPrefix = AbstractMongoDbDao.DEFAULT_COLLECTION_PREFIX;

    private ValueIncrementerFactory incrementerFactory;

    private int maxVarCharLength = AbstractMongoDbDao.DEFAULT_EXIT_MESSAGE_LENGTH;

    private ExecutionContextSerializer serializer;

    @Override
    public void afterPropertiesSet() throws Exception {

        Assert.notNull(mongoTemplate, "mongoTemplate must not be null.");

        if (incrementerFactory == null) {
            incrementerFactory = new MongoDbValueIncrementerFactory(mongoTemplate);
        }

        if (serializer == null) {
            XStreamExecutionContextStringSerializer defaultSerializer = new XStreamExecutionContextStringSerializer();
            defaultSerializer.afterPropertiesSet();

            serializer = defaultSerializer;
        }
    }

    @Override
    protected JobInstanceDao createJobInstanceDao() throws Exception {
        MongoDbJobInstanceDao dao = new MongoDbJobInstanceDao(getEntityInformation(JobInstance.class,mongoTemplate), mongoTemplate);
        dao.setPrefix(collectionPrefix);
        dao.setJobIncrementer(incrementerFactory.getIncrementer(collectionPrefix + "Sequence" + JobInstance.class.getSimpleName()));
        dao.afterPropertiesSet();
        return dao;
    }

    @SuppressWarnings("unchecked")
    private <T, K extends Serializable> MongoEntityInformation<T, K> getEntityInformation(Class<T> clazz,MongoOperations operations) {

     MongoPersistentEntity<?> entity = operations.getConverter().getMappingContext().getPersistentEntity(clazz);
     if (entity == null) {
         throw new MappingException(String.format("Could not lookup mapping metadata for domain class %s!", clazz.getName()));
     }
     return new MappingMongoEntityInformation<T, K>((MongoPersistentEntity<T>) entity);
    }
    
    @Override
    protected JobExecutionDao createJobExecutionDao() throws Exception {
        MongoDbJobExecutionDao dao = new MongoDbJobExecutionDao(getEntityInformation(JobExecution.class,mongoTemplate), mongoTemplate);
        dao.setPrefix(collectionPrefix);
        dao.setJobExecutionIncrementer(incrementerFactory.getIncrementer(collectionPrefix + "Sequence" + JobExecution.class.getSimpleName()));
        dao.setExitMessageLength(maxVarCharLength);
        dao.afterPropertiesSet();
        return dao;
    }

    @Override
    protected StepExecutionDao createStepExecutionDao() throws Exception {
        MongoDbStepExecutionDao dao = new MongoDbStepExecutionDao(getEntityInformation(StepExecution.class,mongoTemplate), mongoTemplate);
        dao.setPrefix(collectionPrefix);
        dao.setStepExecutionIncrementer(incrementerFactory.getIncrementer(collectionPrefix + "Sequence" + StepExecution.class.getSimpleName()));
        dao.setExitMessageLength(maxVarCharLength);
        dao.afterPropertiesSet();
        return dao;
    }

    @Override
    protected ExecutionContextDao createExecutionContextDao() throws Exception {
        MongoDbExecutionContextDao dao = new MongoDbExecutionContextDao(getEntityInformation(ExecutionContext.class,mongoTemplate), mongoTemplate);
        dao.setSerializer(serializer);
        dao.setPrefix(collectionPrefix);
        dao.afterPropertiesSet();
        return dao;
    }

    @Override
    public JobRepository getObject() throws Exception {
        return getTarget();
    }

    private JobRepository getTarget() throws Exception {
        return new SimpleJobRepository(createJobInstanceDao(), createJobExecutionDao(), createStepExecutionDao(), createExecutionContextDao());
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * A custom implementation of the {@link ExecutionContextSerializer}. The default, if not injected, is the
     * {@link org.springframework.batch.core.repository.dao.XStreamExecutionContextStringSerializer}.
     *
     * @see ExecutionContextSerializer
     */
    public void setSerializer(ExecutionContextSerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Public setter for the length of long string columns in database. Note this value will be used for the exit message in both
     * {@link com.github.nmorel.spring.batch.mongodb.repository.dao.MongoDbJobExecutionDao} and {@link com.github.nmorel.spring.batch.mongodb.repository.dao.MongoDbStepExecutionDao} and also the short
     * version of the execution context in {@link com.github.nmorel.spring.batch.mongodb.repository.dao.MongoDbExecutionContextDao}.
     *
     * @param maxVarCharLength the exitMessageLength to set
     */
    public void setMaxVarCharLength(int maxVarCharLength) {
        this.maxVarCharLength = maxVarCharLength;
    }

    /**
     * Public setter for the {@link DB}.
     *
     * @param db a {@link DB}
     */
    public void setMongoTemplate(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    /** Sets the collection prefix for all the batch meta-data tables. */
    public void setCollectionPrefix(String collectionPrefix) {
        this.collectionPrefix = collectionPrefix;
    }
    
}
