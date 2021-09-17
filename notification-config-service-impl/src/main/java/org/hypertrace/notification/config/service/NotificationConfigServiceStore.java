package org.hypertrace.notification.config.service;

import static org.hypertrace.notification.config.service.NotificationConfigServiceConstants.NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME;
import static org.hypertrace.notification.config.service.NotificationConfigServiceConstants.NOTIFICATION_CONFIG_NAMESPACE;

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
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.NewNotificationChannel;
import org.hypertrace.notification.config.service.v1.NewNotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationRule;

public class NotificationConfigServiceStore {
  private final ConfigServiceBlockingStub configServiceBlockingStub;

  public NotificationConfigServiceStore(Channel configChannel) {
    this.configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(configChannel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public NotificationRule createNotificationRule(
      RequestContext requestContext, NewNotificationRule newNotificationRule) {
    NotificationRule notificationRule = getNotificationRule(newNotificationRule);
    return updateNotificationRule(requestContext, notificationRule);
  }

  public NotificationRule updateNotificationRule(
      RequestContext requestContext, NotificationRule notificationRule) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(
                NotificationConfigServiceConstants.NOTIFICATION_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(NOTIFICATION_CONFIG_NAMESPACE)
            .setContext(notificationRule.getId())
            .setConfig(convertNotificationRuleToGeneric(notificationRule))
            .build();
    Value value = upsertConfig(requestContext, upsertConfigRequest);
    return convertFromGenericToNotificationRule(value);
  }

  public List<NotificationRule> getAllNotificationRules(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(
                NotificationConfigServiceConstants.NOTIFICATION_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(NOTIFICATION_CONFIG_NAMESPACE)
            .build();
    return getAllConfigs(requestContext, getAllConfigsRequest).stream()
        .map(
            contextSpecificConfig ->
                convertFromGenericToNotificationRule(contextSpecificConfig.getConfig()))
        .collect(Collectors.toUnmodifiableList());
  }

  public void deleteNotificationRule(RequestContext requestContext, String notificationRuleId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(
                NotificationConfigServiceConstants.NOTIFICATION_RULE_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(NOTIFICATION_CONFIG_NAMESPACE)
            .setContext(notificationRuleId)
            .build();
    deleteConfig(requestContext, deleteConfigRequest);
  }

  public NotificationChannel createNotificationChannel(
      RequestContext requestContext, NewNotificationChannel newNotificationChannel) {
    NotificationChannel notificationChannel = getNotificationChannel(newNotificationChannel);
    return updateNotificationChannel(requestContext, notificationChannel);
  }

  public NotificationChannel updateNotificationChannel(
      RequestContext requestContext, NotificationChannel notificationChannel) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(NOTIFICATION_CONFIG_NAMESPACE)
            .setContext(notificationChannel.getId())
            .setConfig(convertNotificationChannelToGeneric(notificationChannel))
            .build();
    Value value = upsertConfig(requestContext, upsertConfigRequest);
    return convertFromGenericToNotificationChannel(value);
  }

  public List<NotificationChannel> getAllNotificationChannels(RequestContext requestContext) {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(NOTIFICATION_CONFIG_NAMESPACE)
            .build();
    List<ContextSpecificConfig> list = getAllConfigs(requestContext, getAllConfigsRequest);
    return list.stream()
        .map(
            contextSpecificConfig ->
                convertFromGenericToNotificationChannel(contextSpecificConfig.getConfig()))
        .collect(Collectors.toUnmodifiableList());
  }

  public void deleteNotificationChannel(
      RequestContext requestContext, String notificationChannelId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setResourceName(NOTIFICATION_CHANNEL_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(NOTIFICATION_CONFIG_NAMESPACE)
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

  private Value convertNotificationChannelToGeneric(NotificationChannel notificationChannel) {
    try {
      return ConfigProtoConverter.convertToValue(notificationChannel);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private Value convertNotificationRuleToGeneric(NotificationRule notificationRule) {
    try {
      return ConfigProtoConverter.convertToValue(notificationRule);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private NotificationChannel convertFromGenericToNotificationChannel(Value value) {
    NotificationChannel.Builder builder = NotificationChannel.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return builder.build();
  }

  private NotificationRule convertFromGenericToNotificationRule(Value value) {
    NotificationRule.Builder builder = NotificationRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    return builder.build();
  }
}
