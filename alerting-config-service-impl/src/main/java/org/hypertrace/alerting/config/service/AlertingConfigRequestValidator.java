package org.hypertrace.alerting.config.service;

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

public interface AlertingConfigRequestValidator {
  void validateCreateNotificationRuleRequest(
      RequestContext requestContext,
      CreateNotificationRuleRequest request,
      AlertingConfigServiceCoordinator alertingConfigServiceCoordinator);

  void validateUpdateNotificationRuleRequest(
      RequestContext requestContext,
      UpdateNotificationRuleRequest request,
      AlertingConfigServiceCoordinator alertingConfigServiceCoordinator);

  void validateGetAllNotificationRulesRequest(
      RequestContext requestContext, GetAllNotificationRulesRequest request);

  void validateDeleteNotificationRuleRequest(
      RequestContext requestContext, DeleteNotificationRuleRequest request);

  void validateCreateNotificationChannelRequest(
      RequestContext requestContext, CreateNotificationChannelRequest request);

  void validateUpdateNotificationChannelRequest(
      RequestContext requestContext, UpdateNotificationChannelRequest request);

  void validateGetAllNotificationChannelsRequest(
      RequestContext requestContext, GetAllNotificationChannelsRequest request);

  void validateGetNotificationChannelRequest(
      RequestContext requestContext, GetNotificationChannelRequest request);

  void validateDeleteNotificationChannelRequest(
      RequestContext requestContext, DeleteNotificationChannelRequest request);

}