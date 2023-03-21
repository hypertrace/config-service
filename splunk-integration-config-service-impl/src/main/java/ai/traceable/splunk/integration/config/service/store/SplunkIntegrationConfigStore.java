package ai.traceable.splunk.integration.config.service.store;

import ai.traceable.splunk.integration.config.service.api.v1.SplunkIntegration;
import ai.traceable.splunk.integration.config.service.api.v1.SplunkIntegrationsFilter;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class SplunkIntegrationConfigStore
    extends IdentifiedObjectStoreWithFilter<SplunkIntegration, SplunkIntegrationsFilter> {

  private static final String SPLUNK_INTEGRATION_CONFIG_RESOURCE_NAME = "splunk-integration";
  private static final String SPLUNK_INTEGRATION_CONFIG_RESOURCE_NAMESPACE =
      "splunk-integration-config";

  @Inject
  public SplunkIntegrationConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SPLUNK_INTEGRATION_CONFIG_RESOURCE_NAMESPACE,
        SPLUNK_INTEGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<SplunkIntegration> buildDataFromValue(Value value) {
    try {
      SplunkIntegration.Builder splunkIntegrationBuilder = SplunkIntegration.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, splunkIntegrationBuilder);
      return Optional.of(splunkIntegrationBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Splunk Integration from value -> {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(SplunkIntegration data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(SplunkIntegration data) {
    return data.getId();
  }

  @Override
  protected Optional<SplunkIntegration> filterConfigData(
      SplunkIntegration data, SplunkIntegrationsFilter filter) {
    if (filter.getIdsList().stream().anyMatch(id -> id.equals(data.getId()))) {
      return Optional.of(data);
    }
    return Optional.empty();
  }
}
