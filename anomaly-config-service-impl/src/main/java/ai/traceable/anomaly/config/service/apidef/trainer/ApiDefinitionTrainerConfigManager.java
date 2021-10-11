package ai.traceable.anomaly.config.service.apidef.trainer;

import static ai.traceable.anomaly.config.service.apidef.trainer.ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.apidef.trainer.ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiDefinitionTrainerConfigManager implements ConfigManager {
  private final ApiDefinitionTrainerConfig apiDefinitionTrainerConfig;
  private final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private final ApiDefinitionTrainerConfigConverter configConverter;

  @Inject
  public ApiDefinitionTrainerConfigManager(
      ApiDefinitionRegistry apiDefinitionRegistry,
      ApiDefinitionTrainerConfig config,
      ApiDefinitionTrainerConfigConverter configConverter,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    this.apiDefinitionTrainerConfig =
        apiDefinitionRegistry.getApiDefinitionTrainerConfig().toBuilder().mergeFrom(config).build();
    this.configConverter = configConverter;
    this.configServiceBlockingStub = configServiceBlockingStub;
  }

  public ApiDefinitionTrainerConfig getApiDefinitionTrainerConfig(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    /*
     * Precedence Order --> apiConfig > serviceConfig > customerConfig > defaultConfig For example, if
     * apiConfig.disabled = true, we use it; if apiConfig.disabled = false, we use
     * serviceConfig.disabled value and so on.. Similarly for all other config values
     */
    List<String> contextsWithIncreasingPriority = new ArrayList<>();
    contextsWithIncreasingPriority.add(
        requestContext
            .getTenantId()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("Unable to get tenant id from request context")));

    switch (configScope.getScopeCase()) {
      case SCOPE_NOT_SET: // for backward compatibility
      case CUSTOMER_SCOPE:
        break;
      case SERVICE_SCOPE:
        contextsWithIncreasingPriority.add(configScope.getServiceScope().getId());
        break;
      case API_SCOPE:
        contextsWithIncreasingPriority.add(configScope.getApiScope().getServiceScope().getId());
        contextsWithIncreasingPriority.add(configScope.getApiScope().getId());
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }
    return fetchConfig(
            requestContext, contextsWithIncreasingPriority, apiDefinitionTrainerConfig, false)
        .orElse(apiDefinitionTrainerConfig);
  }

  public ApiDefinitionTrainerConfig updateApiDefinitionTrainerConfig(
      RequestContext requestContext,
      List<ApiDefinitionApplierConfig> apiDefinitionApplierConfigs,
      AnomalyConfigScope configScope) {
    String context;

    switch (configScope.getScopeCase()) {
      case SCOPE_NOT_SET: // for backward compatibility
      case CUSTOMER_SCOPE:
        context =
            requestContext
                .getTenantId()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Unable to get tenant id from request context"));
        break;
      case SERVICE_SCOPE:
        context = configScope.getServiceScope().getId();
        break;
      case API_SCOPE:
        context = configScope.getApiScope().getId();
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }

    ApiDefinitionTrainerConfig trainerConfig =
        ApiDefinitionTrainerConfig.newBuilder()
            .addAllApiDefinitionApplierConfigs(apiDefinitionApplierConfigs)
            .build();
    ApiDefinitionTrainerConfig changeToUpsert =
        fetchConfig(requestContext, List.of(context), trainerConfig, true).orElse(trainerConfig);

    return upsertConfig(requestContext, context, changeToUpsert);
  }

  private Optional<ApiDefinitionTrainerConfig> fetchConfig(
      RequestContext requestContext,
      List<String> contextsWithIncreasingPriority,
      ApiDefinitionTrainerConfig trainerConfig,
      boolean isPassedConfigPreferred) {

    return fetchConfigValue(requestContext, contextsWithIncreasingPriority)
        .map(
            value -> {
              try {
                return configConverter.convert(value, trainerConfig, isPassedConfigPreferred);
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private Optional<Value> fetchConfigValue(
      RequestContext requestContext, List<String> contextsWithIncreasingPriority) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .addAllContexts(contextsWithIncreasingPriority)
              .setResourceNamespace(APIDEF_TRAINER_CONFIG_NAMESPACE)
              .setResourceName(APIDEF_TRAINER_CONFIG_RESOURCE_NAME)
              .build();

      Value value =
          requestContext.call(
              () -> configServiceBlockingStub.getConfig(getConfigRequest).getConfig());
      if (value != null && value.getKindCase() != Value.KindCase.KIND_NOT_SET) {
        return Optional.of(value);
      }
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }
    return Optional.empty();
  }

  private ApiDefinitionTrainerConfig upsertConfig(
      RequestContext requestContext, String context, ApiDefinitionTrainerConfig config) {
    UpsertConfigRequest.Builder upsertConfigRequestBuilder;

    try {
      upsertConfigRequestBuilder =
          UpsertConfigRequest.newBuilder()
              .setContext(context)
              .setResourceNamespace(APIDEF_TRAINER_CONFIG_NAMESPACE)
              .setResourceName(APIDEF_TRAINER_CONFIG_RESOURCE_NAME)
              .setConfig(configConverter.convert(config));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert ApiDefinitionTrainerConfig {%s} to config object for context {%s}",
              config, context),
          e);
    }

    UpsertConfigResponse response;
    try {
      response =
          requestContext.call(
              () -> configServiceBlockingStub.upsertConfig(upsertConfigRequestBuilder.build()));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to update ApiDefinitionTrainerConfig {%s} for context {%s}", config, context),
          e);
    }

    try {
      return configConverter.convert(response.getConfig());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert config response {%s} to ApiDefinitionTrainerConfig for context {%s}",
              response, context),
          e);
    }
  }
}
