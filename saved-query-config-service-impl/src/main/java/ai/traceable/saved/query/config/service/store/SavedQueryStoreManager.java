package ai.traceable.saved.query.config.service.store;

import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.saved.query.config.service.DefaultSavedQueryConfig;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.CreateSavedQueryResponse;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryResponse;
import ai.traceable.saved.query.config.service.v1.GetAllSavedQueryUsersResponse;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesResponse;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryResponse;
import ai.traceable.saved.query.config.service.v1.User;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SavedQueryStoreManager {

  private final SavedQueryConfigStore savedQueryConfigStore;
  private final DefaultSavedQueryConfig defaultSavedQueryConfig;
  private final TimestampConverter timestampConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  public SavedQueryStoreManager(
      DeletedSavedQueryConfigStore deletedSavedQueryConfigStore,
      DefaultSavedQueryConfig defaultSavedQueryConfig,
      SavedQueryConfigStore savedQueryConfigStore,
      TimestampConverter timestampConverter,
      UuidGenerator uuidGenerator) {
    this.defaultSavedQueryConfig = defaultSavedQueryConfig;
    this.savedQueryConfigStore = savedQueryConfigStore;
    this.timestampConverter = timestampConverter;
    this.uuidGenerator = uuidGenerator;
  }

  public CreateSavedQueryResponse createSavedQuery(
      RequestContext requestContext, CreateSavedQueryRequest request) {
    User.Builder userBuilder =
        User.newBuilder().setEmailId(requestContext.getEmail().orElseThrow());
    requestContext
        .getName()
        .ifPresent(userBuilder::setName); // name - not mandatory; email - mandatory
    requestContext.getUserId().ifPresent(userBuilder::setId);
    SavedQuery.Builder newSavedQueryBuilder =
        SavedQuery.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setName(request.getName())
            .setScope(request.getScope())
            .setQueryClauses(request.getQueryClauses())
            .setCreatedByUserId(requestContext.getUserId().orElseThrow())
            .setAuthor(userBuilder.build());
    if (request.hasComments()) {
      newSavedQueryBuilder.setComments(request.getComments());
    }
    ContextualConfigObject<SavedQuery> configObject =
        savedQueryConfigStore.upsertObject(requestContext, newSavedQueryBuilder.build());
    SavedQuery savedQuery = buildSavedQueryFromConfigObject(configObject);
    return CreateSavedQueryResponse.newBuilder().setSavedQuery(savedQuery).build();
  }

  public UpdateSavedQueryResponse updateSavedQuery(
      RequestContext requestContext, UpdateSavedQueryRequest request) throws StatusException {
    if (defaultSavedQueryConfig.isDefaultQuery(request.getId())) {
      throw Status.UNIMPLEMENTED
          .withDescription("Edit operation is not supported for default queries")
          .asRuntimeException(requestContext.buildTrailers());
    }

    SavedQuery existingSavedQuery = fetchExistingSavedQueryOrThrow(request.getId(), requestContext);
    checkAuthorPermission(existingSavedQuery, requestContext);

    SavedQuery.Builder updatedSavedQueryBuilder =
        SavedQuery.newBuilder(existingSavedQuery)
            .setName(request.getName())
            .setQueryClauses(request.getQueryClauses());
    if (request.hasComments()) {
      updatedSavedQueryBuilder.setComments(request.getComments());
    }
    ContextualConfigObject<SavedQuery> configObject =
        savedQueryConfigStore.upsertObject(requestContext, updatedSavedQueryBuilder.build());
    SavedQuery savedQuery = buildSavedQueryFromConfigObject(configObject);
    return UpdateSavedQueryResponse.newBuilder().setSavedQuery(savedQuery).build();
  }

  public DeleteSavedQueryResponse deleteSavedQuery(
      RequestContext requestContext, DeleteSavedQueryRequest request) throws StatusException {
    String queryId = request.getId();
    if (defaultSavedQueryConfig.isDefaultQuery(queryId)) {
      throw Status.UNIMPLEMENTED
          .withDescription("Delete operation is not supported for default queries")
          .asRuntimeException();
    }
    SavedQuery existingSavedQuery = fetchExistingSavedQueryOrThrow(request.getId(), requestContext);
    checkAuthorPermission(existingSavedQuery, requestContext);

    this.savedQueryConfigStore
        .deleteObject(requestContext, queryId)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    return DeleteSavedQueryResponse.newBuilder().build();
  }

  public GetSavedQueriesResponse fetchSavedQueries(
      RequestContext requestContext, GetSavedQueriesRequest request) {
    List<SavedQuery> undeletedDefaultQueries =
        defaultSavedQueryConfig.getQueriesForScope(request.getFilter().getScope()).stream()
            .filter(
                query ->
                    request.getFilter().getEmailIdsList().isEmpty()
                        || request
                            .getFilter()
                            .getEmailIdsList()
                            .contains(query.getAuthor().getEmailId()))
            .collect(Collectors.toUnmodifiableList());
    List<SavedQuery> storedSavedQueries =
        savedQueryConfigStore.getAllConfigData(requestContext, request);
    return GetSavedQueriesResponse.newBuilder()
        .addAllSavedQueries(undeletedDefaultQueries)
        .addAllSavedQueries(storedSavedQueries)
        .build();
  }

  public GetAllSavedQueryUsersResponse getAllSavedQueryUsers(RequestContext requestContext) {
    return GetAllSavedQueryUsersResponse.newBuilder()
        .addAllUsers(
            savedQueryConfigStore.getAllConfigData(requestContext).stream()
                .map(
                    data ->
                        data.hasAuthor()
                            ? data.getAuthor()
                            : User.newBuilder().setId(data.getCreatedByUserId()).build())
                .collect(Collectors.toUnmodifiableSet()))
        .build();
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
        .orElseThrow(() -> Status.NOT_FOUND.asException(requestContext.buildTrailers()));
  }

  private void checkAuthorPermission(SavedQuery existingSavedQuery, RequestContext requestContext) {
    String authorEmail = existingSavedQuery.getAuthor().getEmailId();
    if (!authorEmail.isBlank() && !authorEmail.equals(requestContext.getEmail().orElseThrow())) {
      throw Status.PERMISSION_DENIED
          .withDescription("The current user is not the author of the query")
          .asRuntimeException();
    }
  }
}
