package org.hypertrace.label.application.rule.config.service;

import static org.hypertrace.label.application.rule.config.service.LabelApplicationRuleConfigServiceConstants.LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME;
import static org.hypertrace.label.application.rule.config.service.LabelApplicationRuleConfigServiceConstants.LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE;

import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;

public class ConfigServiceCoordinatorImpl implements ConfigServiceCoordinator {
  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public ConfigServiceCoordinatorImpl(Channel configChannel) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Override
  public LabelApplicationRule upsertLabelApplicationRule(
      RequestContext requestContext, LabelApplicationRule labelApplicationRule) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE)
            .setConfig(convertToGenericFromLabelApplicationRule(labelApplicationRule))
            .setContext(labelApplicationRule.getId())
            .build();
    UpsertConfigResponse upsertConfigResponse = upsertConfig(requestContext, upsertConfigRequest);
    return convertToLabelApplicationFromGeneric(upsertConfigResponse.getConfig());
  }

  @Override
  public LabelApplicationRule getLabelApplicationRule(
      RequestContext requestContext, String ruleId) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE)
            .addContexts(ruleId)
            .build();
    GetConfigResponse getConfigResponse = getConfig(requestContext, getConfigRequest);
    return convertToLabelApplicationFromGeneric(getConfigResponse.getConfig());
  }

  @Override
  public List<LabelApplicationRule> getLabelApplicationRules(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE)
            .build();
    GetAllConfigsResponse getAllConfigsResponse =
        getAllConfigs(requestContext, getAllConfigsRequest);
    return getAllConfigsResponse.getContextSpecificConfigsList().stream()
        .map(configResponse -> convertToLabelApplicationFromGeneric(configResponse.getConfig()))
        .collect(Collectors.toList());
  }

  @Override
  public void deleteLabelApplicationRule(RequestContext requestContext, String ruleId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(LABEL_APPLICATION_RULE_CONFIG_RESOURCE_NAMESPACE)
            .setContext(ruleId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  private UpsertConfigResponse upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.upsertConfig(request));
  }

  private GetConfigResponse getConfig(RequestContext context, GetConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.getConfig(request));
  }

  private GetAllConfigsResponse getAllConfigs(
      RequestContext context, GetAllConfigsRequest request) {
    return context.call(() -> configServiceBlockingStub.getAllConfigs(request));
  }

  private DeleteConfigResponse deleteConfig(RequestContext context, DeleteConfigRequest request) {
    return context.call(() -> configServiceBlockingStub.deleteConfig(request));
  }

  @SneakyThrows
  private Value convertToGenericFromLabelApplicationRule(
      LabelApplicationRule labelApplicationRule) {
    return ConfigProtoConverter.convertToValue(labelApplicationRule);
  }

  @SneakyThrows
  private LabelApplicationRule convertToLabelApplicationFromGeneric(Value value) {
    LabelApplicationRule.Builder builder = LabelApplicationRule.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return builder.build();
  }
}
