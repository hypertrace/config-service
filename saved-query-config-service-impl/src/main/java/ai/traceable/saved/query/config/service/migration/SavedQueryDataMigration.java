package ai.traceable.saved.query.config.service.migration;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import ai.traceable.iam.v2.GetUserDetailsByIdRequest;
import ai.traceable.iam.v2.GetUserDetailsByIdResponse;
import ai.traceable.iam.v2.IamServiceGrpc;
import ai.traceable.saved.query.config.service.store.DefaultMongoCollection;
import ai.traceable.saved.query.config.service.store.DefaultMongoDatabase;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import ai.traceable.saved.query.config.service.v1.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.client.result.UpdateResult;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.conversions.Bson;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.core.documentstore.Document;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class SavedQueryDataMigration {
  private final Optional<DefaultMongoDatabase> database;
  private final IamServiceGrpc.IamServiceBlockingStub iamServiceBlockingStub;

  private static final String RESOURCE_FIELD_NAME = "resourceName";
  private static final String SAVED_QUERY_RESOURCE_NAME_VALUE = "saved-query";
  private static final String RESOURCE_NAMESPACE_FIELD_NAME = "resourceNamespace";
  private static final String SAVED_QUERY_CONFIG_RESOURCE_NAMESPACE_VALUE = "saved-query-config";
  private static final String COLLECTION_NAME = "configurations";
  private static final String AUTHOR_ATTRIBUTE = "config.author";
  private static final String DOCUMENT_ID = "_id";

  public void migrate() {
    if (database.isEmpty()) {
      log.error(
          "DefaultMongoDatabase not initialized (perhaps deployment doesn't have mongo). Omitting saved query data migration");
      return;
    }
    DefaultMongoCollection collection = database.get().getMongoCollection(COLLECTION_NAME);
    Iterator<Document> documents = collection.find(createSavedQueryFilter());
    log.info("Starting saved-query user data migration");

    try {
      int noOfdocumentsMigrated = 0;
      while (documents.hasNext()) {
        Document document = documents.next();
        String jsonDocument = document.toJson();
        ConfigDocument configDocument = ConfigDocument.fromJson(jsonDocument);
        String documentId = configDocument.getDocumentId();

        // saved-query mongo document
        SavedQuery savedQuery = null;
        try {
          savedQuery = buildSavedQueryFromConfigDocument(configDocument);
          log.debug("Starting data migration for saved query with id: {}", savedQuery.getId());
        } catch (IllegalArgumentException exception) {
          log.info("Error deserializing saved query document: {} ... skipping", documentId);
        }

        // Fetching user-id from existing document
        String userId =
            savedQuery.hasAuthor()
                ? savedQuery.getAuthor().getId()
                : savedQuery.getCreatedByUserId();
        if (userId.isBlank()) {
          log.info("Blank userId for the query with id: {} ... skipping", savedQuery.getId());
          continue;
        }
        log.debug("User id for the current document: {}", userId);

        // Fetching user information using user-id
        GetUserDetailsByIdResponse userInfo =
            iamServiceBlockingStub.getUserDetailsById(
                GetUserDetailsByIdRequest.newBuilder().setUserId(userId).build());
        User userInformation =
            User.newBuilder()
                .setId(userId)
                .setName(userInfo.getUserDetails().getName())
                .setEmailId(userInfo.getUserDetails().getEmail())
                .build();

        // Upsert user information to the saved-query document.
        Bson update = createUpdateQueryFromUser(userInformation);
        Bson filter = createDocumentIdFilter(documentId);
        UpdateResult updateResult = collection.upsertDocument(filter, update);

        if (updateResult.getModifiedCount() == 1) {
          log.debug("Document updated successfully");
          noOfdocumentsMigrated += 1;
        } else {
          log.debug(
              "Document not updated; some error occurred. Was the update request acknowledged by MongoDB: {}, Number of documents found in MongoDB: {}",
              updateResult.wasAcknowledged(),
              updateResult.getMatchedCount());
        }
        log.info(
            "Data migration ended gracefully. No of documents migrated: {}", noOfdocumentsMigrated);
      }
    } catch (Exception e) {
      log.info("Some error occured while migration user information for saved queries ", e);
    } finally {
      database.ifPresent(DefaultMongoDatabase::closeMongoClient);
    }
  }

  private Bson createUpdateQueryFromUser(User userInformation)
      throws InvalidProtocolBufferException {
    String jsonString = JsonFormat.printer().print(userInformation);
    BasicDBObject object = BasicDBObject.parse(jsonString);
    return new BasicDBObject("$set", new BasicDBObject(AUTHOR_ATTRIBUTE, object));
  }

  private Bson createDocumentIdFilter(String documentId) throws JsonProcessingException {
    return eq(DOCUMENT_ID, documentId);
  }

  private Bson createSavedQueryFilter() {
    return and(
        eq(RESOURCE_FIELD_NAME, SAVED_QUERY_RESOURCE_NAME_VALUE),
        eq(RESOURCE_NAMESPACE_FIELD_NAME, SAVED_QUERY_CONFIG_RESOURCE_NAMESPACE_VALUE));
  }

  private static SavedQuery buildSavedQueryFromConfigDocument(ConfigDocument configDocument)
      throws IOException {
    if (configDocument.getConfig() == null)
      throw new IllegalArgumentException("CONFIG_ATTRIBUTE is missing or null");

    SavedQuery.Builder savedQueryBuilder = SavedQuery.newBuilder();
    ConfigProtoConverter.mergeFromValue(configDocument.getConfig(), savedQueryBuilder);
    return savedQueryBuilder.build();
  }
}
