package ai.traceable.saved.query.config.service.store;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.hypertrace.core.documentstore.Document;

@AllArgsConstructor
@EqualsAndHashCode
public class DefaultMongoCollection {
  private final MongoCollection<BasicDBObject> mongoCollection;

  public Iterator<Document> find(Bson filter) {
    return new DefaultMongoIterator(mongoCollection.find(filter).iterator());
  }

  public UpdateResult upsertDocument(Bson filter, Bson update) {
    return mongoCollection.updateOne(filter, update, new UpdateOptions().upsert(true));
  }

  @AllArgsConstructor
  public static class DefaultMongoIterator implements Iterator<Document> {
    private final Iterator<BasicDBObject> dbObjectIterator;

    @Override
    public boolean hasNext() {
      return dbObjectIterator.hasNext();
    }

    @Override
    public Document next() {
      JsonWriterSettings relaxed =
          JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();
      return () -> this.dbObjectIterator.next().toJson(relaxed);
    }

    @Override
    public void remove() {
      dbObjectIterator.remove();
    }
  }
}
