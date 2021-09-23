package org.hypertrace.notification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.common.base.Preconditions;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannel;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
import org.hypertrace.notification.config.service.v1.NotificationRule;
import org.hypertrace.notification.config.service.v1.NotificationRuleMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;

public class NotificationConfigServiceRequestValidator {

  public void validateCreateNotificationRuleRequest(
      RequestContext requestContext, CreateNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationRuleMutableData(), "NotificationRuleMutableData should be present");
    validateNotificationRuleMutableData(request.getNotificationRuleMutableData());
  }

  public void validateUpdateNotificationRuleRequest(
      RequestContext requestContext, UpdateNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationRule(), "Notification rule should be present");
    validateNonDefaultPresenceOrThrow(
        request.getNotificationRule(), NotificationRule.ID_FIELD_NUMBER);
    validateNotificationRuleMutableData(
        request.getNotificationRule().getNotificationRuleMutableData());
  }

  private void validateNotificationRuleMutableData(NotificationRuleMutableData data) {
    validateNonDefaultPresenceOrThrow(data, NotificationRuleMutableData.RULE_NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(data, NotificationRuleMutableData.CHANNEL_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        data, NotificationRuleMutableData.EVENT_CONDITION_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        data, NotificationRuleMutableData.EVENT_CONDITION_TYPE_FIELD_NUMBER);
  }

  public void validateGetAllNotificationRulesRequest(
      RequestContext requestContext, GetAllNotificationRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteNotificationRuleRequest(
      RequestContext requestContext, DeleteNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, NotificationRule.ID_FIELD_NUMBER);
  }

  public void validateCreateNotificationChannelRequest(
      RequestContext requestContext, CreateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationChannelMutableData(),
        "NotificationChannelMutableData should be present");
    validateNotificationChannelMutableData(request.getNotificationChannelMutableData());
  }

  public void validateUpdateNotificationChannelRequest(
      RequestContext requestContext, UpdateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationChannel(), "Notification channel should be present");
    validateNonDefaultPresenceOrThrow(
        request.getNotificationChannel(), NotificationChannel.ID_FIELD_NUMBER);
    validateNotificationChannelMutableData(
        request.getNotificationChannel().getNotificationChannelMutableData());
  }

  private void validateNotificationChannelMutableData(NotificationChannelMutableData data) {
    validateNonDefaultPresenceOrThrow(
        data, NotificationChannelMutableData.CHANNEL_NAME_FIELD_NUMBER);
    Preconditions.checkArgument(
        data.getEmailChannelConfigCount() != 0 || data.getWebhookChannelConfigCount() != 0,
        "Either email or webhook config should be present");
  }

  public void validateGetAllNotificationChannelsRequest(
      RequestContext requestContext, GetAllNotificationChannelsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteNotificationChannelRequest(
      RequestContext requestContext, DeleteNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteNotificationChannelRequest.NOTIFICATION_CHANNEL_ID_FIELD_NUMBER);
  }
}
