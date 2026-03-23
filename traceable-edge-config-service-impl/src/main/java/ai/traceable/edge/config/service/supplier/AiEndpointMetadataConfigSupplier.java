package ai.traceable.edge.config.service.supplier;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.config.service.TraceableEdgeConfigSupplier;
import ai.traceable.edge.config.service.config.TraceableEdgeConfig;
import ai.traceable.edge.config.service.v1.AgentCapabilities;
import ai.traceable.edge.config.service.v1.AiEndpointMetadataConfig;
import ai.traceable.edge.config.service.v1.ConfigPayloads;
import ai.traceable.edge.config.service.v1.ConfigRequestElement;
import ai.traceable.edge.config.service.v1.ConfigResponseElement;
import ai.traceable.entity.fetcher.cache.StreamingAiEndpointMetadataProvider;
import ai.traceable.entity.fetcher.cache.StreamingAiEndpointMetadataProvider.ApiAiEndpointMetadataDetails;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.Map;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class AiEndpointMetadataConfigSupplier implements TraceableEdgeConfigSupplier {
  private static final String CONFIG_TYPE = "AiEndpointMetadataConfig";
  private static final String SERVICE_NAME = "serviceName";
  private static final String ENVIRONMENT = "environment";

  private final StreamingAiEndpointMetadataProvider aiEndpointMetadataProvider;
  private final TraceableEdgeConfig config;
  private final UuidGenerator uuidGenerator;
  private final FeatureCachingClient featureCachingClient;

  @Override
  public String getConfigType() {
    return CONFIG_TYPE;
  }

  @SneakyThrows
  @Override
  public ConfigResponseElement getConfigs(
      RequestContext requestContext,
      String environment,
      ConfigRequestElement requestElement,
      AgentCapabilities agentCapabilities) {

    if (!config.isAiEndpointMetadataEnabled()
        || !featureCachingClient.isProtectionEngineAiAppProtectionEnabledForTenant(
            requestContext)) {
      log.debug("AI App protection not enabled for tenant: {}", requestContext.getTenantId());
      ConfigPayloads emptyPayloads = ConfigPayloads.getDefaultInstance();
      return ConfigResponseElement.newBuilder()
          .setConfigType(getConfigType())
          .setConfigPayloads(emptyPayloads)
          .addSupportedAgentCapabilities(agentCapabilities)
          .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
          .setHash(uuidGenerator.generateId(emptyPayloads))
          .setEnabled(true)
          .build();
    }

    Map<String, String> additionalFields = agentCapabilities.getAdditionalFieldsMap();

    if (!additionalFields.containsKey(SERVICE_NAME)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing serviceName in agent capabilities")
          .asRuntimeException();
    }

    String serviceName = additionalFields.get(SERVICE_NAME);
    ConfigPayloads configPayloads = buildConfigPayloads(requestContext, serviceName, environment);

    return ConfigResponseElement.newBuilder()
        .setConfigType(getConfigType())
        .setConfigPayloads(configPayloads)
        .addSupportedAgentCapabilities(agentCapabilities)
        .setRefreshAfterDuration(config.getAgentPollingFrequency(getConfigType()))
        .setHash(uuidGenerator.generateId(configPayloads))
        .setEnabled(true)
        .build();
  }

  @NonNull
  private ConfigPayloads buildConfigPayloads(
      RequestContext requestContext, String serviceName, String environment) {

    assertNonNullOrEmpty(serviceName, SERVICE_NAME);
    assertNonNullOrEmpty(environment, ENVIRONMENT);

    Stream<ApiAiEndpointMetadataDetails> apiMetadata =
        aiEndpointMetadataProvider.getAllAiEndpointMetadata(
            requestContext, serviceName, environment);

    AiEndpointMetadataConfig.Builder configBuilder = AiEndpointMetadataConfig.newBuilder();
    apiMetadata.forEach(
        metadata ->
            configBuilder.putAiEndpointMetadata(
                metadata.getApiId(), metadata.getAiEndpointMetadata().toByteString()));
    AiEndpointMetadataConfig data = configBuilder.build();
    if (data.getAiEndpointMetadataMap().isEmpty()) {
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
}
