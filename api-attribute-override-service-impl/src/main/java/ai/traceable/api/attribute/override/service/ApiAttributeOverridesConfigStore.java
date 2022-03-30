package ai.traceable.api.attribute.override.service;

import ai.traceable.api.attribute.override.service.v1.ApiAttributeOverrides;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class ApiAttributeOverridesConfigStore extends IdentifiedObjectStore<ApiAttributeOverrides> {

  @Inject
  public ApiAttributeOverridesConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        ApiAttributeOverrideConstants.RESOURCE_NAMESPACE,
        ApiAttributeOverrideConstants.RESOURCE_NAME);
  }

  @Override
  protected Optional<ApiAttributeOverrides> buildDataFromValue(Value value) {
    try {
      ApiAttributeOverrides.Builder attributeOverridesBuilder = ApiAttributeOverrides.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, attributeOverridesBuilder);
      return Optional.of(attributeOverridesBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize ApiAttributeOverrides from value", e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ApiAttributeOverrides apiAttributeOverrides) {
    return ConfigProtoConverter.convertToValue(apiAttributeOverrides);
  }

  @Override
  protected String getContextFromData(ApiAttributeOverrides apiAttributeOverrides) {
    return apiAttributeOverrides.getApiId();
  }
}
