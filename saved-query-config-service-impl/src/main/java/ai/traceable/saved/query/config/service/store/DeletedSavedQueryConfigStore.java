package ai.traceable.saved.query.config.service.store;

import ai.traceable.saved.query.config.service.v1.DeletedDefaultSavedQuery;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DeletedSavedQueryConfigStore extends IdentifiedObjectStore<DeletedDefaultSavedQuery> {

  private static final String SAVED_QUERY_RESOURCE_NAME = "saved-query";
  private static final String DELETED_DEFAULT_SAVED_QUERY_NAMESPACE = "deleted-default-saved-query";

  @Inject
  public DeletedSavedQueryConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DELETED_DEFAULT_SAVED_QUERY_NAMESPACE,
        SAVED_QUERY_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DeletedDefaultSavedQuery> buildDataFromValue(Value value) {
    try {
      DeletedDefaultSavedQuery.Builder builder = DeletedDefaultSavedQuery.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DeletedDefaultSavedQuery data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(DeletedDefaultSavedQuery data) {
    return data.getId();
  }

  void markDefaultIdDeleted(RequestContext requestContext, String id) {
    upsertObject(requestContext, DeletedDefaultSavedQuery.newBuilder().setId(id).build());
  }

  Set<String> getDeletedDefaultIds(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getContext)
        .collect(Collectors.toUnmodifiableSet());
  }
}
