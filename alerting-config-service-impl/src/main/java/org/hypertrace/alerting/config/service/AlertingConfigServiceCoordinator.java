package org.hypertrace.alerting.config.service;

import java.util.List;
import org.hypertrace.alerting.config.service.v1.NewNotificationChannel;
import org.hypertrace.alerting.config.service.v1.NewNotificationRule;
import org.hypertrace.alerting.config.service.v1.NotificationChannel;
import org.hypertrace.alerting.config.service.v1.NotificationRule;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface AlertingConfigServiceCoordinator {

  NotificationRule createNotificationRule(
      RequestContext requestContext, NewNotificationRule newNotificationRule);

  NotificationRule updateNotificationRule(
      RequestContext requestContext, NotificationRule notificationRule);

  List<NotificationRule> getAllNotificationRules(RequestContext requestContext);

  void deleteNotificationRule(RequestContext requestContext, String notificationChannelId);

  NotificationChannel createNotificationChannel(
      RequestContext requestContext, NewNotificationChannel newNotificationChannel);

  NotificationChannel updateNotificationChannel(
      RequestContext requestContext, NotificationChannel notificationChannel);

  List<NotificationChannel> getAllNotificationChannels(RequestContext requestContext);

  NotificationChannel getNotificationChannel(RequestContext requestContext, String channelId);

  void deleteNotificationChannel(RequestContext requestContext, String notificationChannelId);
}
