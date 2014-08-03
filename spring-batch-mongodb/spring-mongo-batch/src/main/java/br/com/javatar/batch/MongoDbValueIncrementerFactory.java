package br.com.javatar.batch;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.Assert;

/**
 * MongoDB implementation of the {@link ValueIncrementerFactory} interface.
 */
public class MongoDbValueIncrementerFactory implements ValueIncrementerFactory {
    /** The MongoDB database */
    private MongoTemplate mongoTemplate;

    public MongoDbValueIncrementerFactory(MongoTemplate mongoTemplate) {
        Assert.notNull(mongoTemplate, "mongoTemplate must not be null");
        this.mongoTemplate = mongoTemplate;
    }

    public ValueIncrementer getIncrementer(String incrementerName) {
        Assert.notNull(incrementerName);
        MongoDbValueIncrementer incrementer = new MongoDbValueIncrementer(mongoTemplate, incrementerName);
        incrementer.afterPropertiesSet();
        return incrementer;
    }
}
