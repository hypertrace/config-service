package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.AUTOMATIC_SECRET_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.REDACTION_RULES_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
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

  @Override
  public RedactionRule createRedactionRule(
      RequestContext requestContext, NewRedactionRule newRedactionRule) {
    RedactionRule redactionRule = getRedactionRule(newRedactionRule);
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(redactionRule.getId())
            .setConfig(new RedactionRuleConfig(redactionRule).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return redactionRule;
  }

  @Override
  public RedactionRule updateRedactionRule(
      RequestContext requestContext, RedactionRule redactionRule) {
    long creationTimestamp =
        getRedactionRuleConfig(requestContext, redactionRule.getId()).getCreationTimestamp();
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(redactionRule.getId())
            .setConfig(new RedactionRuleConfig(redactionRule, creationTimestamp).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return redactionRule;
  }

  @Override
  public List<RedactionRule> getAllRedactionRules(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .build();
    return getAllConfigs(requestContext, getAllConfigsRequest).stream()
        .map(
            contextSpecificConfig ->
                RedactionRuleConfig.fromValue(contextSpecificConfig.getConfig()))
        .sorted()
        .map(RedactionRuleConfig::getRedactionRule)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void deleteRedactionRule(RequestContext requestContext, String redactionRuleId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .setContext(redactionRuleId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
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

  private List<ContextSpecificConfig> getAllConfigs(
      RequestContext context, GetAllConfigsRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
            context.getRequestHeaders(), () -> configServiceBlockingStub.getAllConfigs(request))
        .getContextSpecificConfigsList();
  }

  private void deleteConfig(RequestContext context, DeleteConfigRequest request) {
    GrpcClientRequestContextUtil.executeWithHeadersContext(
        context.getRequestHeaders(), () -> configServiceBlockingStub.deleteConfig(request));
  }

  private RedactionRule getRedactionRule(NewRedactionRule newRedactionRule) {
    String ruleId = UUID.randomUUID().toString();
    RedactionRule.Builder builder =
        RedactionRule.newBuilder()
            .setId(ruleId)
            .setName(newRedactionRule.getName())
            .setDescription(newRedactionRule.getDescription())
            .setCategory(newRedactionRule.getCategory())
            .setRedactionStrategy(newRedactionRule.getRedactionStrategy())
            .setMatchType(newRedactionRule.getMatchType())
            .setRegex(newRedactionRule.getRegex());
    if (newRedactionRule.hasComplexData()) {
      builder.setComplexData(newRedactionRule.getComplexData());
    }
    return builder.build();
  }

  private RedactionRuleConfig getRedactionRuleConfig(
      RequestContext requestContext, String redactionRuleId) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(REDACTION_RULES_CONFIG)
            .setResourceNamespace(SENSITIVE_DATA_CONFIGURATION)
            .addContexts(redactionRuleId)
            .build();
    return RedactionRuleConfig.fromValue(getConfig(requestContext, getConfigRequest));
  }
}
