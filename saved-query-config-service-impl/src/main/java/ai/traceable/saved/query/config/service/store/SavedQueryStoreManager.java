package ai.traceable.saved.query.config.service.store;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryResponse;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryResponse;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesResponse;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SavedQueryStoreManager {

  private final SavedQueryConfigStore savedQueryConfigStore;
  private final TimestampConverter timestampConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  public SavedQueryStoreManager(
      SavedQueryConfigStore savedQueryConfigStore,
      TimestampConverter timestampConverter,
      UuidGenerator uuidGenerator) {
    this.savedQueryConfigStore = savedQueryConfigStore;
    this.timestampConverter = timestampConverter;
    this.uuidGenerator = uuidGenerator;
  }

  public CreateSavedQueryResponse createSavedQuery(
      RequestContext requestContext, CreateSavedQueryRequest request) {
    SavedQuery newSavedQuery =
        SavedQuery.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setName(request.getName())
            .setScope(request.getScope())
            .setQueryClauses(request.getQueryClauses())
            .setCreatedByUserId(requestContext.getUserId().orElseThrow())
            .build();
    ContextualConfigObject<SavedQuery> configObject =
        savedQueryConfigStore.upsertObject(requestContext, newSavedQuery);
    SavedQuery savedQuery = buildSavedQueryFromConfigObject(configObject);
    return CreateSavedQueryResponse.newBuilder().setSavedQuery(savedQuery).build();
  }

  public UpdateSavedQueryResponse updateSavedQuery(
      RequestContext requestContext, UpdateSavedQueryRequest request) throws StatusException {
    SavedQuery existingSavedQuery = fetchExistingSavedQueryOrThrow(request.getId(), requestContext);
    SavedQuery updatedSavedQuery =
        SavedQuery.newBuilder(existingSavedQuery)
            .setName(request.getName())
            .setQueryClauses(request.getQueryClauses())
            .build();
    ContextualConfigObject<SavedQuery> configObject =
        savedQueryConfigStore.upsertObject(requestContext, updatedSavedQuery);
    SavedQuery savedQuery = buildSavedQueryFromConfigObject(configObject);
    return UpdateSavedQueryResponse.newBuilder().setSavedQuery(savedQuery).build();
  }

  public DeleteSavedQueryResponse deleteSavedQuery(
      RequestContext requestContext, DeleteSavedQueryRequest request) {
    this.savedQueryConfigStore
        .deleteObject(requestContext, request.getId())
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    return DeleteSavedQueryResponse.newBuilder().build();
  }

  public GetSavedQueriesResponse fetchSavedQueries(
      RequestContext requestContext, GetSavedQueriesRequest request) {
    List<SavedQuery> savedQueries = savedQueryConfigStore.getAllConfigData(requestContext, request);
    return GetSavedQueriesResponse.newBuilder().addAllSavedQueries(savedQueries).build();
  }

  private SavedQuery buildSavedQueryFromConfigObject(
      ContextualConfigObject<SavedQuery> configObject) {
    return SavedQuery.newBuilder(configObject.getData())
        .setCreatedTimestamp(timestampConverter.convert(configObject.getCreationTimestamp()))
        .setUpdatedTimestamp(timestampConverter.convert(configObject.getLastUpdatedTimestamp()))
        .build();
  }

  private SavedQuery fetchExistingSavedQueryOrThrow(String id, RequestContext requestContext)
      throws StatusException {
    return savedQueryConfigStore
        .getData(requestContext, id)
        .orElseThrow(Status.NOT_FOUND::asException);
  }
}
