package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.ApiEndpointModelConfig;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider;
import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider.ApiEndpointModelDetails;
import ai.traceable.entity.fetcher.cache.StreamingApiEndpointModelProvider.ModelTypeFilter;
import ai.traceable.entity.fetcher.cache.config.ApiEndpointModelFetchConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.grpc.Status;
import java.util.Map;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@Singleton
@AllArgsConstructor(onConstructor_ = @Inject)
public class ApiEndpointModelConfigSupplier implements TraceableEdgeConfigSupplier {

  private static final String CONFIG_TYPE = "ApiEndpointModelConfig";
  private static final String SERVICE_NAME = "serviceName";
  private static final String ENVIRONMENT = "environment";
  private static final String MODEL_TYPE_FILTER = "modelTypeFilter";

  private final StreamingApiEndpointModelProvider apiEndpointModelProvider;
  private final TraceableEdgeConfig traceableEdgeConfig;
  private final UuidGenerator uuidGenerator;
  private final ApiEndpointModelFetchConfig fetchConfig;

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {

    Map<String, String> additionalFields = agentCapabilities.getAdditionalFieldsMap();

    if (!additionalFields.containsKey(SERVICE_NAME)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing serviceName in agent capabilities")
          .asRuntimeException();
    }

    String serviceName = additionalFields.get(SERVICE_NAME);
    ModelTypeFilter modelTypeFilter = parseModelTypeFilter(additionalFields);

    ConfigPayloads configPayloads =
        buildConfigPayloads(requestContext, serviceName, environment, modelTypeFilter);

    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setConfigPayloads(configPayloads)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(traceableEdgeConfig.getAgentPollingFrequency(getConfigType()))
        .setHash(uuidGenerator.generateId(configPayloads))
        .setEnabled(true)
        .build();
  }

  private ConfigPayloads buildConfigPayloads(
      RequestContext requestContext,
      String serviceName,
      String environment,
      ModelTypeFilter modelTypeFilter) {

    assertNonNullOrEmpty(serviceName, SERVICE_NAME);
    assertNonNullOrEmpty(environment, ENVIRONMENT);

    Stream<ApiEndpointModelDetails> modelDetails =
        apiEndpointModelProvider.getApiEndpointModels(
            requestContext, serviceName, environment, fetchConfig, modelTypeFilter);

    ApiEndpointModelConfig.Builder configBuilder = ApiEndpointModelConfig.newBuilder();
    modelDetails.forEach(
        details ->
            configBuilder.putApiEndpointModels(
                details.getApiId(), details.getApiDetectionModel().toByteString()));

    ApiEndpointModelConfig data = configBuilder.build();
    if (data.getApiEndpointModelsMap().isEmpty()) {
      return ConfigPayloads.getDefaultInstance();
    }
    return ConfigPayloads.newBuilder().addConfigBytes(data.toByteString()).build();
  }

  private static void assertNonNullOrEmpty(String value, String fieldName) {
    if (value == null || value.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("%s is null or empty", fieldName))
          .asRuntimeException();
    }
  }

  private static ModelTypeFilter parseModelTypeFilter(Map<String, String> additionalFields) {
    if (!additionalFields.containsKey(MODEL_TYPE_FILTER)) {
      return ModelTypeFilter.BOTH;
    }
    String filterValue = additionalFields.get(MODEL_TYPE_FILTER);
    try {
      return ModelTypeFilter.valueOf(filterValue.toUpperCase());
    } catch (IllegalArgumentException e) {
      return ModelTypeFilter.BOTH;
    }
  }
}
