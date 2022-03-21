package ai.traceable.waf.provider.integration.service;

import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class WafIntegrationStore extends IdentifiedObjectStore<WafIntegration> {
  private static final String WAF_INTEGRATION_CONFIG_RESOURCE_NAME = "waf-integration-config";
  private static final String WAF_INTEGRATION_CONFIG_RESOURCE_NAMESPACE =
      "waf-provider-integration";

  @Inject
  WafIntegrationStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        WAF_INTEGRATION_CONFIG_RESOURCE_NAMESPACE,
        WAF_INTEGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<WafIntegration> buildDataFromValue(Value value) {
    try {
      WafIntegration.Builder builder = WafIntegration.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(WafIntegration object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(WafIntegration object) {
    return object.getId();
  }
}
