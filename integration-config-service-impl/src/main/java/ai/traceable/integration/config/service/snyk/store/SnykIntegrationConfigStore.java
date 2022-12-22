package ai.traceable.integration.config.service.snyk.store;

import static ai.traceable.integration.config.service.constants.IntegrationServiceConstants.INTEGRATION_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.integration.config.service.snyk.v1.SnykIntegration;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class SnykIntegrationConfigStore extends DefaultObjectStore<SnykIntegration> {

  private static final String SNYK_INTEGRATION_CONFIG_RESOURCE_NAME = "snyk-integration";

  @Inject
  public SnykIntegrationConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        INTEGRATION_CONFIG_RESOURCE_NAMESPACE,
        SNYK_INTEGRATION_CONFIG_RESOURCE_NAME);
  }

  @Override
  protected Optional<SnykIntegration> buildDataFromValue(Value value) {
    try {
      SnykIntegration.Builder snykIntegrationBuilder = SnykIntegration.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, snykIntegrationBuilder);
      return Optional.of(snykIntegrationBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Snyk Integration from value -> {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(SnykIntegration snykIntegration) {
    return ConfigProtoConverter.convertToValue(snykIntegration);
  }
}
