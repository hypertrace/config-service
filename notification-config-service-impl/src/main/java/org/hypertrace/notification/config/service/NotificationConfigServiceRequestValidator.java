package org.hypertrace.notification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.common.base.Preconditions;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.NotificationChannelMutableData;
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
    Preconditions.checkArgument(
        !request.getNotificationRule().getId().isBlank(), "Id cannot be empty");
    validateNotificationRuleMutableData(request.getNotificationRule().getNotificationRuleMutableData());
  }

  private void validateNotificationRuleMutableData(NotificationRuleMutableData data) {
    Preconditions.checkArgument(
        !data.getRuleName().isBlank(),
        "Rule name cannot be empty");
    Preconditions.checkArgument(
        !data.getChannelId().isBlank(),
        "ChannelId cannot be empty");
    Preconditions.checkArgument(
        !data.getEventConditionId().isBlank(),
        "EventConditionId cannot be empty");
    Preconditions.checkArgument(
        !data.getEventConditionType().isBlank(),
        "EventConditionType cannot be empty");
  }

  public void validateGetAllNotificationRulesRequest(
      RequestContext requestContext, GetAllNotificationRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteNotificationRuleRequest(
      RequestContext requestContext, DeleteNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        !request.getNotificationRuleId().isBlank(), "RuleId cannot be empty");
  }

  public void validateCreateNotificationChannelRequest(
      RequestContext requestContext, CreateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationChannelMutableData(), "NotificationChannelMutableData should be present");
    validateNotificationChannelMutableData(request.getNotificationChannelMutableData());
  }

  public void validateUpdateNotificationChannelRequest(
      RequestContext requestContext, UpdateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationChannel(), "Notification channel should be present");
    Preconditions.checkArgument(
        !request.getNotificationChannel().getId().isBlank(), "Id cannot be empty");
    validateNotificationChannelMutableData(request.getNotificationChannel().getNotificationChannelMutableData());
  }

  private void validateNotificationChannelMutableData(NotificationChannelMutableData data) {
    Preconditions.checkArgument(
        !data.getChannelName().isBlank(),
        "Channel name should be present");
    Preconditions.checkArgument(
        data.getEmailChannelConfigCount() != 0
            || data.getWebhookChannelConfigCount() != 0,
        "Either email or webhook config should be present");
  }

  public void validateGetAllNotificationChannelsRequest(
      RequestContext requestContext, GetAllNotificationChannelsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateDeleteNotificationChannelRequest(
      RequestContext requestContext, DeleteNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        !request.getNotificationChannelId().isBlank(), "ChannelId cannot be empty");
  }
}
