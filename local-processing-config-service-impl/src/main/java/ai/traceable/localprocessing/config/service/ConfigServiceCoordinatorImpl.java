package ai.traceable.localprocessing.config.service;

import static ai.traceable.localprocessing.config.service.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAME;
import static ai.traceable.localprocessing.config.service.LocalProcessingConstants.LOCAL_PROCESSING_RULE_RESOURCE_NAMESPACE;

import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleMetadata;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {

  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public ConfigServiceCoordinatorImpl(Channel configChannel) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
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

  private void validateRule(LocalProcessingRule localProcessingRule) {
    Preconditions.checkArgument(
        localProcessingRule.getProtectionMode() != ProtectionMode.PROTECTION_MODE_UNSPECIFIED,
        "Protection mode can't be unspecified");
  }

  private UpsertConfigResponse upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        context.getRequestHeaders(), () -> configServiceBlockingStub.upsertConfig(request));
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
