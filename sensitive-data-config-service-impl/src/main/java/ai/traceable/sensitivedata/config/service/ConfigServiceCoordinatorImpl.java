package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.Status;
import java.util.Optional;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {

  static final String DEFAULT_PARAM_TYPE_REDACTION_STRATEGY =
      "default.param.type.redaction.strategy";
  static final String DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED =
      "default.automatic.secret.redaction.enabled";

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final RedactionStrategy defaultParamTypeRedactionStrategy;
  private final boolean defaultAutomaticSecretRedactionEnabled;

  public ConfigServiceCoordinatorImpl(Channel configChannel, Config config) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.defaultParamTypeRedactionStrategy =
        RedactionStrategy.valueOf(config.getString(DEFAULT_PARAM_TYPE_REDACTION_STRATEGY));
    this.defaultAutomaticSecretRedactionEnabled =
        config.getBoolean(DEFAULT_AUTOMATIC_SECRET_REDACTION_ENABLED);
  }

  @Override
  public void upsertParamTypeRedactionStrategyConfig(
      RequestContext requestContext,
      ParamType paramType,
      ParamTypeRedactionStrategyConfig paramTypeRedactionStrategyConfig) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(paramType.name())
            .setConfig(paramTypeRedactionStrategyConfig.toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
  }

  @Override
  public RedactionStrategy getParamTypeRedactionStrategy(
      RequestContext requestContext, ParamType paramType) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .addContexts(paramType.name())
            .build();
    Optional<ParamTypeRedactionStrategyConfig> paramTypeRedactionStrategyConfig =
        ParamTypeRedactionStrategyConfig.fromValue(getConfig(requestContext, getConfigRequest));
    return paramTypeRedactionStrategyConfig.isPresent()
        ? paramTypeRedactionStrategyConfig.get().getRedactionStrategy()
        : defaultParamTypeRedactionStrategy;
  }

  @Override
  public void upsertAutomaticSecretRedactionStrategyConfig(
      RequestContext requestContext,
      AutomaticSecretRedactionStrategyConfig automaticSecretRedactionStrategyConfig) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setConfig(automaticSecretRedactionStrategyConfig.toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
  }

  @Override
  public boolean isAutomaticSecretRedactionStrategyEnabled(RequestContext requestContext) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .build();
    Optional<AutomaticSecretRedactionStrategyConfig> automaticSecretRedactionStrategyConfig =
        AutomaticSecretRedactionStrategyConfig.fromValue(
            getConfig(requestContext, getConfigRequest));
    return automaticSecretRedactionStrategyConfig
      .map(AutomaticSecretRedactionStrategyConfig::isEnabled)
      .orElse(defaultAutomaticSecretRedactionEnabled);
  }

  private Value upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
            context.getRequestHeaders(), () -> configServiceBlockingStub.upsertConfig(request))
        .getConfig();
  }

  private Value getConfig(RequestContext context, GetConfigRequest request) {
    try {
      return GrpcClientRequestContextUtil.executeWithHeadersContext(
              context.getRequestHeaders(), () -> configServiceBlockingStub.getConfig(request))
          .getConfig();
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Value.getDefaultInstance();
      }
      throw e;
    }
  }
}
