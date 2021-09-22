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
import org.hypertrace.notification.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;

public class NotificationConfigServiceRequestValidator {

  public void validateCreateNotificationRuleRequest(
      RequestContext requestContext, CreateNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNewNotificationRule(), "Notification rule should be present");
    Preconditions.checkArgument(
        !request.getNewNotificationRule().getNotificationRuleData().getRuleName().isBlank(),
        "Rule name cannot be empty");
    Preconditions.checkArgument(
        !request.getNewNotificationRule().getNotificationRuleData().getChannelId().isBlank(),
        "ChannelId cannot be empty");
  }

  public void validateUpdateNotificationRuleRequest(
      RequestContext requestContext, UpdateNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationRule(), "Notification rule should be present");
    Preconditions.checkArgument(
        !request.getNotificationRule().getNotificationRuleData().getRuleName().isBlank(),
        "Rule name cannot be empty");
    Preconditions.checkArgument(
        !request.getNotificationRule().getNotificationRuleData().getChannelId().isBlank(),
        "ChannelId cannot be empty");
    Preconditions.checkArgument(
        !request.getNotificationRule().getId().isBlank(), "Id cannot be empty");
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
        request.hasNewNotificationChannel(), "Notification channel should be present");
    Preconditions.checkArgument(
        !request.getNewNotificationChannel().getNotificationChannelMutableData().getChannelName().isBlank(),
        "Channel name should be present");
    Preconditions.checkArgument(
        request.getNewNotificationChannel().getNotificationChannelMutableData().hasNotificationChannelConfig(),
        "Channel config has to be present");
  }

  public void validateUpdateNotificationChannelRequest(
      RequestContext requestContext, UpdateNotificationChannelRequest request) {
    validateRequestContextOrThrow(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationChannel(), "Notification channel should be present");
    Preconditions.checkArgument(
        !request.getNotificationChannel().getNotificationChannelMutableData().getChannelName().isBlank(),
        "Channel name should be present");
    Preconditions.checkArgument(
        request.getNotificationChannel().getNotificationChannelMutableData().hasNotificationChannelConfig(),
        "Channel config has to be present");
    Preconditions.checkArgument(
        !request.getNotificationChannel().getId().isBlank(), "Id cannot be empty");
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
