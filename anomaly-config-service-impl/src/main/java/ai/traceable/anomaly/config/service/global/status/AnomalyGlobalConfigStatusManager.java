package ai.traceable.anomaly.config.service.global.status;

import static ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants.ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyGlobalConfigStatusManager implements ConfigStatusManager {

  private final AnomalyGlobalConfigServiceConfig config;
  private final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private final GlobalConfigStatusConverter configConverter;

  @Inject
  public AnomalyGlobalConfigStatusManager(
      AnomalyGlobalConfigServiceConfig config,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      GlobalConfigStatusConverter configConverter) {
    this.config = config;
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.configConverter = configConverter;
  }

  @Override
  public AnomalyConfigStatus getAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope configScope) {

    /*
     * Precedence Order --> apiConfig > serviceConfig > customerConfig > defaultConfig For example, if
     * apiConfig.disabled = true, we use it; if apiConfig.disabled = false, we use
     * serviceConfig.disabled value and so on.. Similarly for internal flag too..
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

    return fetchConfig(requestContext, contextsWithIncreasingPriority)
        .orElse(config.getConfigStatus());
  }

  @Override
  public AnomalyConfigStatusChange updateAnomalyConfigStatus(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      AnomalyConfigStatusChange configStatusChange) {

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

    AnomalyConfigStatusChange changeToUpsert =
        fetchContextSpecificConfig(requestContext, context)
            .map(AnomalyConfigStatusChange::toBuilder)
            .map(builder -> builder.mergeFrom(configStatusChange))
            .map(AnomalyConfigStatusChange.Builder::build)
            .orElse(configStatusChange);

    return upsertConfig(requestContext, context, changeToUpsert);
  }

  private Optional<AnomalyConfigStatus> fetchConfig(
      RequestContext requestContext, List<String> contextsWithIncreasingPriority) {

    return fetchConfigValue(requestContext, contextsWithIncreasingPriority)
        .map(
            value -> {
              try {
                return configConverter.convert(value, config.getConfigStatus());
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private Optional<AnomalyConfigStatusChange> fetchContextSpecificConfig(
      RequestContext requestContext, String context) {

    return fetchConfigValue(requestContext, List.of(context))
        .map(
            value -> {
              try {
                return configConverter.convert(value);
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
              .setResourceNamespace(ANOMALY_GLOBAL_CONFIG_NAMESPACE)
              .setResourceName(ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
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

  private AnomalyConfigStatusChange upsertConfig(
      RequestContext requestContext, String context, AnomalyConfigStatusChange configStatusChange) {
    UpsertConfigRequest.Builder upsertConfigRequestBuilder;

    try {
      upsertConfigRequestBuilder =
          UpsertConfigRequest.newBuilder()
              .setContext(context)
              .setResourceNamespace(ANOMALY_GLOBAL_CONFIG_NAMESPACE)
              .setResourceName(ANOMALY_GLOBAL_CONFIG_STATUS_RESOURCE_NAME)
              .setConfig(configConverter.convert(configStatusChange));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert anomaly config status change {%s} to config object for context {%s}",
              configStatusChange, context),
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
              "Unable to update anomaly config status change {%s} for context {%s}",
              configStatusChange, context),
          e);
    }

    try {
      return configConverter.convert(response.getConfig());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert config response {%s} to anomaly config status for context {%s}",
              response, context),
          e);
    }
  }

  private boolean isEmpty(String str) {
    return str == null || str.isEmpty();
  }
}
