package ai.traceable.localprocessing.config.service;

import static ai.traceable.localprocessing.config.service.LocalProcessingConstants.DEFAULT_PROTECTION_MODE_CONFIG;
import static ai.traceable.localprocessing.config.service.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAME;
import static ai.traceable.localprocessing.config.service.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE;

import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleMetadata;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {

  static final String LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG = "local.processing.config.service";
  static final String DEFAULT_PROTECTION_MODE = "default.protection.mode";

  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final ProtectionMode defaultProtectionMode;

  public ConfigServiceCoordinatorImpl(Channel configChannel, Config config) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    this.defaultProtectionMode =
        ProtectionMode.valueOf(
            config
                .getConfig(LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG)
                .getString(DEFAULT_PROTECTION_MODE));
  }

  @Override
  public LocalProcessingRuleDetails createLocalProcessingRule(
      RequestContext requestContext, NewLocalProcessingRule newLocalProcessingRule) {
    LocalProcessingRule localProcessingRule = buildLocalProcessingRule(newLocalProcessingRule);
    validateRule(localProcessingRule);
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(LOCAL_PROCESSING_RULE_RESOURCE_NAME)
            .setResourceNamespace(LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE)
            .setContext(localProcessingRule.getId())
            .setConfig(convertToGeneric(localProcessingRule))
            .build();
    UpsertConfigResponse upsertConfigResponse = upsertConfig(requestContext, upsertConfigRequest);
    return buildLocalProcessingRuleDetails(
        localProcessingRule, upsertConfigResponse.getCreationTimestamp());
  }

  @Override
  public LocalProcessingRuleDetails updateLocalProcessingRule(
      RequestContext requestContext, LocalProcessingRule localProcessingRule) {
    validateRule(localProcessingRule);
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(LOCAL_PROCESSING_RULE_RESOURCE_NAME)
            .setResourceNamespace(LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE)
            .setContext(localProcessingRule.getId())
            .setConfig(convertToGeneric(localProcessingRule))
            .build();
    UpsertConfigResponse upsertConfigResponse = upsertConfig(requestContext, upsertConfigRequest);
    return buildLocalProcessingRuleDetails(
        localProcessingRule, upsertConfigResponse.getCreationTimestamp());
  }

  @Override
  public List<LocalProcessingRuleDetails> getAllLocalProcessingRules(
      RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(LOCAL_PROCESSING_RULE_RESOURCE_NAME)
            .setResourceNamespace(LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE)
            .build();
    return getAllConfigs(requestContext, getAllConfigsRequest).stream()
        .map(
            contextSpecificConfig ->
                buildLocalProcessingRuleDetails(
                    convertFromGeneric(contextSpecificConfig.getConfig()),
                    contextSpecificConfig.getCreationTimestamp()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void deleteLocalProcessingRule(
      RequestContext requestContext, String localProcessingRuleId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(LOCAL_PROCESSING_RULE_RESOURCE_NAME)
            .setResourceNamespace(LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE)
            .setContext(localProcessingRuleId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  @Override
  public ProtectionMode upsertDefaultProtectionModeConfig(
      RequestContext requestContext, ProtectionMode defaultProtectionMode) {

    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(DEFAULT_PROTECTION_MODE_CONFIG)
            .setResourceNamespace(LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE)
            .setConfig(DefaultProtectionModeConfigConverter.toValue(defaultProtectionMode))
            .build();
    return DefaultProtectionModeConfigConverter.fromValue(
            upsertConfig(requestContext, upsertConfigRequest).getConfig())
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  @Override
  public ProtectionMode getDefaultProtectionModeConfig(RequestContext requestContext) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(DEFAULT_PROTECTION_MODE_CONFIG)
            .setResourceNamespace(LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE)
            .build();
    Optional<ProtectionMode> defaultProtectionModeConfig =
        getConfig(requestContext, getConfigRequest);
    return defaultProtectionModeConfig.orElse(defaultProtectionMode);
  }

  private void validateRule(LocalProcessingRule localProcessingRule) {
    Preconditions.checkArgument(
        localProcessingRule.getProtectionMode() != ProtectionMode.PROTECTION_MODE_UNSPECIFIED,
        "Protection mode can't be unspecified");
  }

  private UpsertConfigResponse upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        context.getRequestHeaders(), () -> configServiceBlockingStub.upsertConfig(request));
  }

  private Optional<ProtectionMode> getConfig(RequestContext context, GetConfigRequest request) {
    try {
      return DefaultProtectionModeConfigConverter.fromValue(
          context.call(() -> configServiceBlockingStub.getConfig(request)).getConfig());
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
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

  private LocalProcessingRule buildLocalProcessingRule(
      NewLocalProcessingRule newLocalProcessingRule) {
    String ruleId = UUID.randomUUID().toString();
    return LocalProcessingRule.newBuilder()
        .setId(ruleId)
        .setUrlPattern(newLocalProcessingRule.getUrlPattern())
        .setHostHeader(newLocalProcessingRule.getHostHeader())
        .setProtectionMode(newLocalProcessingRule.getProtectionMode())
        .build();
  }

  private LocalProcessingRuleDetails buildLocalProcessingRuleDetails(
      LocalProcessingRule localProcessingRule, long creationTimestamp) {
    return LocalProcessingRuleDetails.newBuilder()
        .setRule(localProcessingRule)
        .setMetadata(
            LocalProcessingRuleMetadata.newBuilder()
                .setCreationTimestamp(creationTimestamp)
                .build())
        .build();
  }

  private Value convertToGeneric(LocalProcessingRule localProcessingRule) {
    try {
      return ConfigProtoConverter.convertToValue(localProcessingRule);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private LocalProcessingRule convertFromGeneric(Value config) {
    LocalProcessingRule.Builder builder = LocalProcessingRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(config, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return builder.build();
  }
}
