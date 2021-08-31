package ai.traceable.risk.config.service.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import com.google.protobuf.Message;
import io.grpc.Status;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

public abstract class RiskConfigServiceDao<M extends Message> {

  private final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private RiskConfigConverter<M> configConverter;
  private final RiskConfigUtils<M> configUtils;

  protected RiskConfigServiceDao(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<M> configConverter,
      RiskConfigUtils<M> configUtils) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.configConverter = configConverter;
    this.configUtils = configUtils;
  }

  @SneakyThrows
  public Optional<M> fetchConfig(RequestContext requestContext) {
    GetConfigResponse response;
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .setResourceNamespace(getConfigNamespace())
              .setResourceName(getConfigResourceName())
              .build();
      response = requestContext.call(() -> configServiceBlockingStub.getConfig(getConfigRequest));
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }

    try {
      return Optional.of(
          configConverter.convert(response.getConfig(), configUtils.getNewBuilder()));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert config response {%s} to config proto for customer {%s}",
              response, requestContext.getTenantId()),
          e);
    }
  }

  public M upsertConfig(RequestContext requestContext, M config) {
    UpsertConfigRequest.Builder upsertConfigRequestBuilder;

    try {
      upsertConfigRequestBuilder =
          UpsertConfigRequest.newBuilder()
              .setResourceNamespace(getConfigNamespace())
              .setResourceName(getConfigResourceName())
              .setConfig(configConverter.convert(config));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert config proto {%s} to config object for customer {%s}",
              config, requestContext.getTenantId()),
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
              "Unable to upsert config proto {%s} for customer {%s}",
              config, requestContext.getTenantId()),
          e);
    }

    try {
      return configConverter.convert(response.getConfig(), configUtils.getNewBuilder());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert config response {%s} to config proto for customer {%s}",
              response, requestContext.getTenantId()),
          e);
    }
  }

  @SneakyThrows
  public void deleteConfig(RequestContext requestContext) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceNamespace(getConfigNamespace())
            .setResourceName(getConfigResourceName())
            .build();
    try {
      requestContext.call(() -> configServiceBlockingStub.deleteConfig(deleteConfigRequest));
    } catch (Exception e) {
      if (!Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        throw e;
      }
    }
  }

  protected abstract String getConfigResourceName();

  private String getConfigNamespace() {
    return RiskConfigConstants.RISK_CONFIG_NAMESPACE;
  }
}
