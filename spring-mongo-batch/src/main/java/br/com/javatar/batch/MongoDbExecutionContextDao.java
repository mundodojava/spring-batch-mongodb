package br.com.javatar.batch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.util.Assert;

import com.google.common.base.Charsets;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/** {@link org.springframework.batch.core.repository.dao.ExecutionContextDao} implementation for MongoDB */
public class MongoDbExecutionContextDao extends AbstractMongoDbDao<ExecutionContext,Long> implements ExecutionContextDao {
	
	private static final String COLLECTION_NAME = ExecutionContext.class.getSimpleName();
	
	private static final String SERIALIZED_CONTEXT_KEY = "serializedContext";
	
    public MongoDbExecutionContextDao(MongoEntityInformation<ExecutionContext, Serializable> metadata,
			MongoOperations mongoOperations) {
		super(metadata, mongoOperations);
	}

    private ExecutionContextSerializer serializer;

    /** Setter for {@link org.springframework.core.serializer.Serializer} implementation */
    public void setSerializer(ExecutionContextSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        getCollection().createIndex(BasicDBObjectBuilder.start().add(STEP_EXECUTION_ID_KEY, 1).add(JOB_EXECUTION_ID_KEY, 1).get());
    }

    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        return getExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId());
    }

    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        return getExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId());
    }

    private ExecutionContext getExecutionContext(String executionIdKey, Long executionId) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        DBObject result = getCollection().findOne(new BasicDBObject(executionIdKey, executionId));
        return deserializeContext(result);
    }

    private ExecutionContext deserializeContext(DBObject dbObject) {
        ExecutionContext executionContext = new ExecutionContext();
        if (dbObject != null) {
            Object value = dbObject.get(SERIALIZED_CONTEXT_KEY);
            if (null != value) {
                Map<String, Object> map;
                try {
                    ByteArrayInputStream in = new ByteArrayInputStream(value.toString().getBytes(Charsets.UTF_8));
                    map = serializer.deserialize(in);
                } catch (IOException ioe) {
                    throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
                }
                for(Map.Entry<String, Object> entry : map.entrySet()) {
                    executionContext.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return executionContext;
    }

    public void saveExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    public void saveExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
        for(StepExecution stepExecution : stepExecutions) {
            saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
        }
    }

    public void updateExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    public void updateExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    private void saveOrUpdateExecutionContext(String executionIdKey, Long executionId, ExecutionContext executionContext) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        Assert.notNull(executionContext, "The ExecutionContext must not be null.");

        DBObject dbObject = new BasicDBObject(executionIdKey, executionId);
        dbObject.put(SERIALIZED_CONTEXT_KEY, serializeContext(executionContext));

        getCollection().update(new BasicDBObject(executionIdKey, executionId), dbObject, true, false);
    }

    private String serializeContext(ExecutionContext ctx) {
        Map<String, Object> m = new HashMap<String, Object>();
        for(Map.Entry<String, Object> me : ctx.entrySet()) {
            m.put(me.getKey(), me.getValue());
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            serializer.serialize(m, out);
            return new String(out.toByteArray(), Charsets.UTF_8);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Could not serialize the execution context", ioe);
        }
    }

    @Override
    protected String getCollectionName() {
        return COLLECTION_NAME;
    }
}
