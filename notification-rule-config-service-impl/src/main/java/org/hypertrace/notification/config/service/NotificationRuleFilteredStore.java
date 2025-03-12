package org.hypertrace.notification.config.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Channel;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleFilter;

@Slf4j
public class NotificationRuleFilteredStore
    extends IdentifiedObjectStoreWithFilter<NotificationRule, NotificationRuleFilter> {

  private static final String NOTIFICATION_CONFIG_NAMESPACE = "notification-v1";
  private static final String NOTIFICATION_RULE_CONFIG_RESOURCE_NAME = "notificationRuleConfig";

  public NotificationRuleFilteredStore(
      Channel channel, ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        ConfigServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get()),
        NOTIFICATION_CONFIG_NAMESPACE,
        NOTIFICATION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<NotificationRule> buildDataFromValue(Value value) {
    NotificationRule.Builder builder = NotificationRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Conversion failed. value {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(NotificationRule object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(NotificationRule object) {
    return object.getId();
  }

  @Override
  protected Optional<NotificationRule> filterConfigData(
      NotificationRule data, NotificationRuleFilter filter) {
    return Optional.of(data)
        .filter(rule -> satisfiesEnabledPredicate(rule, filter))
        .filter(rule -> satisfiesConditionTypePredicate(rule, filter));
  }

  private boolean satisfiesEnabledPredicate(
      NotificationRule notificationRule, NotificationRuleFilter filter) {
    return !filter.hasEnabled()
        || filter.getEnabled() != notificationRule.getNotificationRuleMutableData().getDisabled();
  }

  private boolean satisfiesConditionTypePredicate(
      NotificationRule notificationRule, NotificationRuleFilter filter) {
    return filter.getEventConditionTypeList().isEmpty()
        || filter
            .getEventConditionTypeList()
            .contains(notificationRule.getNotificationRuleMutableData().getEventConditionType());
  }
}
