package ai.traceable.saved.query.config.service.store;

import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.SavedQuery;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class SavedQueryConfigStore
    extends IdentifiedObjectStoreWithFilter<SavedQuery, GetSavedQueriesRequest> {

  private static final String SAVED_QUERY_RESOURCE_NAME = "saved-query";
  private static final String SAVED_QUERY_CONFIG_RESOURCE_NAMESPACE = "saved-query-config";

  @Inject
  public SavedQueryConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SAVED_QUERY_CONFIG_RESOURCE_NAMESPACE,
        SAVED_QUERY_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<SavedQuery> buildDataFromValue(Value value) {
    SavedQuery.Builder savedQueryBuilder = SavedQuery.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, savedQueryBuilder);
    return Optional.of(savedQueryBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(SavedQuery savedQuery) {
    return ConfigProtoConverter.convertToValue(savedQuery);
  }

  @Override
  protected String getContextFromData(SavedQuery savedQuery) {
    return savedQuery.getId();
  }

  @Override
  protected Optional<SavedQuery> filterConfigData(SavedQuery data, GetSavedQueriesRequest request) {
    if (request.getFilter().hasScope() && !request.getFilter().getScope().equals(data.getScope())) {
      return Optional.empty();
    }
    if (!request.getFilter().getEmailIdsList().isEmpty()) {
      return Optional.of(data.getAuthor().getEmailId())
          .filter(email -> request.getFilter().getEmailIdsList().contains(email))
          .map(author -> data);
    } else if (!request.getFilter().getUserIdsList().isEmpty()) {
      String userId = data.hasAuthor() ? data.getAuthor().getId() : data.getCreatedByUserId();
      return request.getFilter().getUserIdsList().contains(userId)
          ? Optional.of(data)
          : Optional.empty();
    }
    return Optional.of(data);
  }
}
