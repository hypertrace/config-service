package org.hypertrace.notification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.notification.config.service.v1.CreateNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.DeleteNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.GetAllNotificationRulesRequest;
import org.hypertrace.notification.config.service.v1.GetNotificationRuleRequest;
import org.hypertrace.notification.config.service.v1.NotificationIntegrationTarget;
import org.hypertrace.notification.config.service.v1.NotificationRuleMutableData;
import org.hypertrace.notification.config.service.v1.UpdateNotificationRuleRequest;

public class NotificationRuleConfigServiceRequestValidator {

  public void validateCreateNotificationRuleRequest(
      RequestContext requestContext, CreateNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNotificationRuleMutableData(request.getNotificationRuleMutableData());
  }

  public void validateUpdateNotificationRuleRequest(
      RequestContext requestContext, UpdateNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateNotificationRuleRequest.ID_FIELD_NUMBER);
    validateNotificationRuleMutableData(request.getNotificationRuleMutableData());
  }

  private void validateNotificationRuleMutableData(NotificationRuleMutableData data) {
    validateNonDefaultPresenceOrThrow(data, NotificationRuleMutableData.RULE_NAME_FIELD_NUMBER);
    if (data.hasIntegrationTarget()) {
      validateNonDefaultPresenceOrThrow(
          data.getIntegrationTarget(), NotificationIntegrationTarget.INTEGRATION_ID_FIELD_NUMBER);
      validateNonDefaultPresenceOrThrow(
          data.getIntegrationTarget(), NotificationIntegrationTarget.TYPE_FIELD_NUMBER);
      if (data.hasChannelId()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Channel Id should not be populated with integration target.")
            .asRuntimeException();
      }
    } else {
      validateNonDefaultPresenceOrThrow(data, NotificationRuleMutableData.CHANNEL_ID_FIELD_NUMBER);
    }
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
    validateNonDefaultPresenceOrThrow(
        request, DeleteNotificationRuleRequest.NOTIFICATION_RULE_ID_FIELD_NUMBER);
  }

  public void validateGetNotificationRuleRequest(
      RequestContext requestContext, GetNotificationRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, GetNotificationRuleRequest.NOTIFICATION_RULE_ID_FIELD_NUMBER);
  }
}
