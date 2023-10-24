package ai.traceable.syslog.integration.config.service.store;

import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegration;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationsFilter;
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
public class SyslogIntegrationConfigStore
    extends IdentifiedObjectStoreWithFilter<
        SyslogServerIntegration, SyslogServerIntegrationsFilter> {
  private static final String SYSLOG_INTEGRATION_CONFIG_RESOURCE_NAME = "syslog-integration";
  private static final String SYSLOG_INTEGRATION_CONFIG_RESOURCE_NAMESPACE =
      "syslog-integration-config";

  @Inject
  public SyslogIntegrationConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        SYSLOG_INTEGRATION_CONFIG_RESOURCE_NAMESPACE,
        SYSLOG_INTEGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<SyslogServerIntegration> buildDataFromValue(Value value) {
    try {
      SyslogServerIntegration.Builder syslogIntegrationBuilder =
          SyslogServerIntegration.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, syslogIntegrationBuilder);
      return Optional.of(syslogIntegrationBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize Syslog Integration from value -> {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(SyslogServerIntegration data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(SyslogServerIntegration data) {
    return data.getId();
  }

  @Override
  protected Optional<SyslogServerIntegration> filterConfigData(
      SyslogServerIntegration data, SyslogServerIntegrationsFilter filter) {
    if (filter.getIdsList().stream().anyMatch(id -> id.equals(data.getId()))) {
      return Optional.of(data);
    }
    return Optional.empty();
  }
}
