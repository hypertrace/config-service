package org.hypertrace.alerting.config.service;

import static org.hypertrace.alerting.config.service.AlertingConfigServiceConstants.ALERTING_CHANNEL_CONFIG_RESOURCE_NAME;
import static org.hypertrace.alerting.config.service.AlertingConfigServiceConstants.ALERTING_CONFIG_NAMESPACE;
import static org.hypertrace.alerting.config.service.AlertingConfigServiceConstants.ALERTING_RULE_CONFIG_RESOURCE_NAME;

import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.alerting.config.service.v1.NewNotificationChannel;
import org.hypertrace.alerting.config.service.v1.NewNotificationRule;
import org.hypertrace.alerting.config.service.v1.NotificationChannel;
import org.hypertrace.alerting.config.service.v1.NotificationRule;
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

public class AlertingConfigServiceCoordinatorImpl implements AlertingConfigServiceCoordinator {
  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public AlertingConfigServiceCoordinatorImpl(Channel configChannel) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Override
  public NotificationRule createNotificationRule(
      RequestContext requestContext, NewNotificationRule newNotificationRule) {
    NotificationRule notificationRule = getNotificationRule(newNotificationRule);
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(ALERTING_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(notificationRule.getId())
            .setConfig(new NotificationRuleWrapper(notificationRule).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return notificationRule;
  }

  @Override
  public NotificationRule updateNotificationRule(
      RequestContext requestContext, NotificationRule notificationRule) {
    long creationTimestamp =
        getNotificationRuleConfigWrapper(requestContext, notificationRule.getId())
            .getCreationTimestamp();
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(ALERTING_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(notificationRule.getId())
            .setConfig(new NotificationRuleWrapper(notificationRule, creationTimestamp).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return notificationRule;
  }

  @Override
  public List<NotificationRule> getAllNotificationRules(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(ALERTING_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .build();
    return getAllConfigs(requestContext, getAllConfigsRequest).stream()
        .map(
            contextSpecificConfig ->
                NotificationRuleWrapper.fromValue(contextSpecificConfig.getConfig()))
        .sorted()
        .map(NotificationRuleWrapper::getNotificationRule)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void deleteNotificationRule(RequestContext requestContext, String notificationRuleId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(ALERTING_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(notificationRuleId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  @Override
  public NotificationChannel createNotificationChannel(
      RequestContext requestContext, NewNotificationChannel newNotificationChannel) {
    NotificationChannel notificationChannel = getNotificationChannel(newNotificationChannel);
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(ALERTING_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(notificationChannel.getId())
            .setConfig(new NotificationChannelWrapper(notificationChannel).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return notificationChannel;
  }

  @Override
  public NotificationChannel updateNotificationChannel(
      RequestContext requestContext, NotificationChannel notificationChannel) {
    long creationTimestamp =
        getNotificationChannelConfigWrapper(requestContext, notificationChannel.getId())
            .getCreationTimestamp();
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(ALERTING_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(notificationChannel.getId())
            .setConfig(
                new NotificationChannelWrapper(notificationChannel, creationTimestamp).toValue())
            .build();
    upsertConfig(requestContext, upsertConfigRequest);
    return notificationChannel;
  }

  @Override
  public List<NotificationChannel> getAllNotificationChannels(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(ALERTING_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .build();
    return getAllConfigs(requestContext, getAllConfigsRequest).stream()
        .map(
            contextSpecificConfig ->
                NotificationChannelWrapper.fromValue(contextSpecificConfig.getConfig()))
        .sorted()
        .map(NotificationChannelWrapper::getNotificationChannel)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public NotificationChannel getNotificationChannel(
      RequestContext requestContext, String notificationChannelId) {
    return getNotificationChannelConfigWrapper(requestContext, notificationChannelId)
        .getNotificationChannel();
  }

  @Override
  public void deleteNotificationChannel(
      RequestContext requestContext, String notificationChannelId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(ALERTING_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .setContext(notificationChannelId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  private Value upsertConfig(RequestContext context, UpsertConfigRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        context.getRequestHeaders(), () -> configServiceBlockingStub.upsertConfig(request))
        .getConfig();
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

  private Value getConfig(RequestContext context, GetConfigRequest request) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        context.getRequestHeaders(), () -> configServiceBlockingStub.getConfig(request))
        .getConfig();
  }

  private NotificationRule getNotificationRule(NewNotificationRule newNotificationRule) {
    String ruleId = UUID.randomUUID().toString();
    NotificationRule.Builder builder =
        NotificationRule.newBuilder()
            .setId(ruleId)
            .setEnvironment(newNotificationRule.getEnvironment())
            .setRuleName(newNotificationRule.getRuleName())
            .setEventConditionId(newNotificationRule.getEventConditionId())
            .setEventConditionType(newNotificationRule.getEventConditionType())
            .setDescription(newNotificationRule.getDescription())
            .setChannelId(newNotificationRule.getChannelId())
            .setRateLimitIntervalDuration(newNotificationRule.getRateLimitIntervalDuration());
    return builder.build();
  }

  private NotificationChannel getNotificationChannel(
      NewNotificationChannel newNotificationChannel) {
    String ruleId = UUID.randomUUID().toString();
    NotificationChannel.Builder builder =
        NotificationChannel.newBuilder()
            .setId(ruleId)
            .setChannelName(newNotificationChannel.getChannelName())
            .setNotificationChannelConfig(newNotificationChannel.getNotificationChannelConfig());
    return builder.build();
  }

  private NotificationRuleWrapper getNotificationRuleConfigWrapper(
      RequestContext requestContext, String notificationRuleId) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(ALERTING_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .addContexts(notificationRuleId)
            .build();
    return NotificationRuleWrapper.fromValue(getConfig(requestContext, getConfigRequest));
  }

  private NotificationChannelWrapper getNotificationChannelConfigWrapper(
      RequestContext requestContext, String notificationChannelId) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .setResourceName(ALERTING_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ALERTING_CONFIG_NAMESPACE)
            .addContexts(notificationChannelId)
            .build();
    return NotificationChannelWrapper.fromValue(getConfig(requestContext, getConfigRequest));
  }
}
