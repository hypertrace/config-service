package ai.traceable.http.event.collector.integration.config.service.store;

import ai.traceable.http.event.collector.integration.config.service.v1.HttpEventCollectorIntegration;
import ai.traceable.http.event.collector.integration.config.service.v1.HttpEventCollectorIntegrationsFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class HttpEventCollectorIntegrationConfigServiceStore
    extends IdentifiedObjectStoreWithFilter<
        HttpEventCollectorIntegration, HttpEventCollectorIntegrationsFilter> {

  public static final String HTTP_EVENT_COLLECTOR_INTEGRATION_RESOURCE_NAMESPACE =
      "http_event_collector_integration_config";
  public static final String HTTP_EVENT_COLLECTOR_INTEGRATION_RESOURCE_NAME =
      "http_event_collector_integration";

  @Inject
  public HttpEventCollectorIntegrationConfigServiceStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        HTTP_EVENT_COLLECTOR_INTEGRATION_RESOURCE_NAMESPACE,
        HTTP_EVENT_COLLECTOR_INTEGRATION_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<HttpEventCollectorIntegration> filterConfigData(
      HttpEventCollectorIntegration data, HttpEventCollectorIntegrationsFilter filter) {
    if (filter.getIdsList().stream().anyMatch(id -> id.equals(data.getId()))) {
      return Optional.of(data);
    }
    return Optional.empty();
  }

  @Override
  protected Optional<HttpEventCollectorIntegration> buildDataFromValue(Value value) {
    try {
      HttpEventCollectorIntegration.Builder httpEventCollectorIntegrationBuilder =
          HttpEventCollectorIntegration.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, httpEventCollectorIntegrationBuilder);
      return Optional.of(httpEventCollectorIntegrationBuilder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error(
          "Could not deserialize Http Event Collector Integration from value -> {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(HttpEventCollectorIntegration data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(HttpEventCollectorIntegration data) {
    return data.getId();
  }
}
