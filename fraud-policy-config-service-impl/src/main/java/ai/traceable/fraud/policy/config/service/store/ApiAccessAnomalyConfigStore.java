package ai.traceable.fraud.policy.config.service.store;

import ai.traceable.fraud.policy.config.service.v1.ApiAccessAnomalyConfig;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class ApiAccessAnomalyConfigStore extends IdentifiedObjectStore<ApiAccessAnomalyConfig> {
  public static final String FRAUD_CONFIG_RESOURCE_NAMESPACE =
      "api-access-anomaly-config-resource-namespace";

  public static final String FRAUD_CONFIG_RESOURCE = "api-access-anomaly-config-resource";

  @Inject
  public ApiAccessAnomalyConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        FRAUD_CONFIG_RESOURCE_NAMESPACE,
        FRAUD_CONFIG_RESOURCE,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<ApiAccessAnomalyConfig> buildDataFromValue(Value value) {
    ApiAccessAnomalyConfig.Builder builder = ApiAccessAnomalyConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ApiAccessAnomalyConfig apiAccessAnomalyConfig) {
    return ConfigProtoConverter.convertToValue(apiAccessAnomalyConfig);
  }

  @Override
  protected String getContextFromData(ApiAccessAnomalyConfig apiAccessAnomalyConfig) {
    return apiAccessAnomalyConfig.getId();
  }
}
