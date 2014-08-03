package br.com.javatar.batch;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

/** Implementation of {@link ValueIncrementer} that uses MongoDB. */
public class MongoDbValueIncrementer implements ValueIncrementer, InitializingBean {
    /** The MongoDB database */
    private MongoTemplate mongoTemplate;

    /** The name of the sequence/table containing the sequence */
    private String incrementerName;

    /** The length to which a string result should be pre-pended with zeroes */
    private int paddingLength = 0;

    public MongoDbValueIncrementer() {
    }

    public MongoDbValueIncrementer(MongoTemplate mongoTemplate, String incrementerName) {
        this.mongoTemplate = mongoTemplate;
        this.incrementerName = incrementerName;
    }

    public void afterPropertiesSet() {
        Assert.notNull(mongoTemplate, "Property 'mongoTemplate' is required");
        Assert.notNull(incrementerName, "Property 'incrementerName' is required");
    }

    public MongoTemplate getMongoTemplate() {
        return mongoTemplate;
    }

    public void setMongoTemplate(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public String getIncrementerName() {
        return incrementerName;
    }

    public void setIncrementerName(String incrementerName) {
        this.incrementerName = incrementerName;
    }

    public int getPaddingLength() {
        return paddingLength;
    }

    public void setPaddingLength(int paddingLength) {
        this.paddingLength = paddingLength;
    }

    public int nextIntValue() throws DataAccessException {
        return (int) getNextKey();
    }

    public long nextLongValue() throws DataAccessException {
        return getNextKey();
    }

    public String nextStringValue() throws DataAccessException {
        String s = Long.toString(getNextKey());
        int len = s.length();
        if (len < this.paddingLength) {
            StringBuilder sb = new StringBuilder(this.paddingLength);
            for(int i = 0 ; i < this.paddingLength - len ; i++) {
                sb.append('0');
            }
            sb.append(s);
            s = sb.toString();
        }
        return s;
    }

    /**
     * Determine the next key to use, as a long.
     *
     * @return the key to use as a long. It will eventually be converted later in another format by the public concrete methods of this class.
     */
    protected long getNextKey() {
        DBCollection collection = mongoTemplate.getCollection(incrementerName);
        BasicDBObject sequence = new BasicDBObject();
        collection.update(sequence, new BasicDBObject("$inc", new BasicDBObject("value", 1L)), true, false);
        return (Long) collection.findOne(sequence).get("value");
    }
}
