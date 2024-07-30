package ai.traceable.saved.query.config.service.store;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultMongoDatabase {
  private static final String GENERIC_CONFIG_SERVICE = "generic.config.service";
  private static final String DOCUMENT_STORE = "document.store";
  private static final String DATA_STORE_TYPE = "dataStoreType";
  private static final String MONGODB_DATA_STORE = "mongo";
  private static final String MONGODB_CONNECTOR_URL = "url";
  private static final String MONGODB_HOST = "host";
  private static final String MONGODB_PORT = "port";
  private static final String DEFAULT_DB = "default_db";

  private final com.mongodb.client.MongoDatabase database;
  private final MongoClient mongoClient;

  DefaultMongoDatabase(MongoDatabase database, MongoClient mongoClient) {
    this.database = database;
    this.mongoClient = mongoClient;
  }

  private final ConcurrentHashMap<String, DefaultMongoCollection> collectionConcurrentHashMap =
      new ConcurrentHashMap<>();

  public DefaultMongoCollection getMongoCollection(String collectionName) {
    collectionConcurrentHashMap.putIfAbsent(
        collectionName,
        new DefaultMongoCollection(
            this.database.getCollection(collectionName, BasicDBObject.class)));
    return collectionConcurrentHashMap.get(collectionName);
  }

  public void closeMongoClient() {
    this.mongoClient.close();
  }

  public static DefaultMongoDatabase from(Config serviceConfig) {
    Config genericConfigService = serviceConfig.getConfig(GENERIC_CONFIG_SERVICE).resolve();
    Config documentStoreConfig = genericConfigService.getConfig(DOCUMENT_STORE).resolve();
    if (!documentStoreConfig.getString(DATA_STORE_TYPE).equals(MONGODB_DATA_STORE)) {
      return null;
    }
    Config mongoConfig = documentStoreConfig.getConfig(MONGODB_DATA_STORE);
    ConnectionString connString;
    if (mongoConfig.hasPath(MONGODB_CONNECTOR_URL)) {
      connString = new ConnectionString(mongoConfig.getString(MONGODB_CONNECTOR_URL));
    } else {
      String hostName = mongoConfig.getString(MONGODB_HOST);
      int port = mongoConfig.getInt(MONGODB_PORT);
      connString = new ConnectionString("mongodb://" + hostName + ":" + port);
    }

    MongoClientSettings settings =
        MongoClientSettings.builder().applyConnectionString(connString).retryWrites(true).build();
    MongoClient client = MongoClients.create(settings);
    return new DefaultMongoDatabase(client.getDatabase(DEFAULT_DB), client);
  }
}
