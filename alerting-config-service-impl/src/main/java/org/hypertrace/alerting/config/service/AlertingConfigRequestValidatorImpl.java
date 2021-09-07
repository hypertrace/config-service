package org.hypertrace.alerting.config.service;

import com.google.common.base.Preconditions;
import org.hypertrace.alerting.config.service.v1.CreateNotificationChannelRequest;
import org.hypertrace.alerting.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.alerting.config.service.v1.DeleteNotificationChannelRequest;
import org.hypertrace.alerting.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.alerting.config.service.v1.GetAllNotificationChannelsRequest;
import org.hypertrace.alerting.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.alerting.config.service.v1.GetNotificationChannelRequest;
import org.hypertrace.alerting.config.service.v1.UpdateNotificationChannelRequest;
import org.hypertrace.alerting.config.service.v1.UpdateNotificationRuleRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AlertingConfigRequestValidatorImpl implements AlertingConfigRequestValidator {
  @Override
  public void validateCreateNotificationRuleRequest(
      RequestContext requestContext,
      CreateNotificationRuleRequest request,
      AlertingConfigServiceCoordinator alertingConfigServiceCoordinator) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        request.hasNewNotificationRule(), "Notification rule should be present");
    Preconditions.checkArgument(
        !request.getNewNotificationRule().getRuleName().isBlank(), "Rule name cannot be empty");
    Preconditions.checkArgument(
        !request.getNewNotificationRule().getChannelId().isBlank(), "ChannelId cannot be empty");
    alertingConfigServiceCoordinator.getNotificationChannel(
        requestContext, request.getNewNotificationRule().getChannelId());
  }

  @Override
  public void validateUpdateNotificationRuleRequest(
      RequestContext requestContext,
      UpdateNotificationRuleRequest request,
      AlertingConfigServiceCoordinator alertingConfigServiceCoordinator) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationRule(), "Notification rule should be present");
    Preconditions.checkArgument(
        !request.getNotificationRule().getRuleName().isBlank(), "Rule name cannot be empty");
    Preconditions.checkArgument(
        !request.getNotificationRule().getChannelId().isBlank(), "ChannelId cannot be empty");
    alertingConfigServiceCoordinator.getNotificationChannel(
        requestContext, request.getNotificationRule().getChannelId());
  }

  @Override
  public void validateGetAllNotificationRulesRequest(
      RequestContext requestContext, GetAllNotificationRulesRequest request) {
    validateTenantID(requestContext);
  }

  @Override
  public void validateDeleteNotificationRuleRequest(
      RequestContext requestContext, DeleteNotificationRuleRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        !request.getNotificationRuleId().isBlank(), "RuleId cannot be empty");
  }

  @Override
  public void validateCreateNotificationChannelRequest(
      RequestContext requestContext, CreateNotificationChannelRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        request.hasNewNotificationChannel(), "Notification channel should be present");
    Preconditions.checkArgument(
        !request.getNewNotificationChannel().getChannelName().isBlank(),
        "Channel name should be present");
    Preconditions.checkArgument(
        request.getNewNotificationChannel().hasNotificationChannelConfig(),
        "Channel config has to be present");
  }

  @Override
  public void validateUpdateNotificationChannelRequest(
      RequestContext requestContext, UpdateNotificationChannelRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        request.hasNotificationChannel(), "Notification channel should be present");
    Preconditions.checkArgument(
        !request.getNotificationChannel().getChannelName().isBlank(),
        "Channel name should be present");
    Preconditions.checkArgument(
        request.getNotificationChannel().hasNotificationChannelConfig(),
        "Channel config has to be present");
  }

  @Override
  public void validateGetAllNotificationChannelsRequest(
      RequestContext requestContext, GetAllNotificationChannelsRequest request) {
    validateTenantID(requestContext);
  }

  @Override
  public void validateGetNotificationChannelRequest(
      RequestContext requestContext, GetNotificationChannelRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        !request.getNotificationChannelId().isBlank(), "ChannelId cannot be empty");
  }

  @Override
  public void validateDeleteNotificationChannelRequest(
      RequestContext requestContext, DeleteNotificationChannelRequest request) {
    validateTenantID(requestContext);
    Preconditions.checkArgument(
        !request.getNotificationChannelId().isBlank(), "ChannelId cannot be empty");
  }

  private void validateTenantID(RequestContext requestContext) {
    Preconditions.checkArgument(
        requestContext.getTenantId().isPresent(), "Tenant ID is not present in request context");
  }
}